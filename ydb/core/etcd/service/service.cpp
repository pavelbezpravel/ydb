#include "ydb/core/etcd/service/service.h"

#include <ydb/core/etcd/kv/events.h>
#include <ydb/core/etcd/kv/kv_table_create.h>
#include <ydb/core/etcd/kv/kv.h>

#include <ydb/core/etcd/revision/events.h>
#include <ydb/core/etcd/revision/revision_table_create.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/library/table_creator/table_creator.h>
#include "ydb/library/services/services.pb.h"

#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_PROXY, "[ydb] [ETCD_FEATURE] [EtcdService]: " << stream)

namespace NYdb::NEtcd {

class TEtcdService : public NActors::TActorBootstrapped<TEtcdService> {
public:
    void Bootstrap() {
        Become(&TEtcdService::CreateTablesStateFunc);
    }

private:
    STRICT_STFUNC(CreateTablesStateFunc,
        hFunc(TEvEtcdRevision::TEvCreateTableResponse, Handle)
        hFunc(TEvEtcdKV::TEvCreateTableResponse, Handle)
        hFunc(TEvEtcdKV::TEvCompactionRequest, Store)
        hFunc(TEvEtcdKV::TEvDeleteRangeRequest, Store)
        hFunc(TEvEtcdKV::TEvPutRequest, Store)
        hFunc(TEvEtcdKV::TEvRangeRequest, Store)
        hFunc(TEvEtcdKV::TEvTxnRequest, Store)
    )

    void CreateTables() {
        if (TablesCreating > 0) {
            return;
        }
        CreateRevisionTable();
        CreateKVTable();
    }

    void CreateRevisionTable() {
        ++TablesCreating;
        Register(NYdb::NEtcd::CreateRevisionTableCreateActor(kLogComponent, Path));
    }

    void Handle(TEvEtcdRevision::TEvCreateTableResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            LOG_E("Cannot create EtcdService, Issues: " << ev->Get()->Issues.ToOneLineString());
            return;
        }
        ProcessStored();
    }

    void CreateKVTable() {
        ++TablesCreating;
        Register(NYdb::NEtcd::CreateKVTableCreateActor(kLogComponent, Path));
    }

    void Handle(TEvEtcdKV::TEvCreateTableResponse::TPtr& ev) {
        Y_UNUSED(ev);
        ProcessStored();
    }

    void ProcessStored() {
        Y_ABORT_UNLESS(TablesCreating > 0);
        if (--TablesCreating > 0) {
            return;
        }
        Become(&TEtcdService::StateFunc);
        for (const auto& request : StoredRequests) {
            std::visit([&](const auto& arg) {
                Send(new NActors::IEventHandle(SelfId(), arg->Sender, arg->Release().Release()));
            }, request);
        }
        StoredRequests.clear();
    }

    void Store(TEvEtcdKV::TEvCompactionRequest::TPtr& ev) {
        CreateTables();
        StoredRequests.emplace_back(ev);
    }

    void Store(TEvEtcdKV::TEvDeleteRangeRequest::TPtr& ev) {
        CreateTables();
        StoredRequests.emplace_back(ev);
    }

    void Store(TEvEtcdKV::TEvPutRequest::TPtr& ev) {
        CreateTables();
        StoredRequests.emplace_back(ev);
    }

    void Store(TEvEtcdKV::TEvRangeRequest::TPtr& ev) {
        CreateTables();
        StoredRequests.emplace_back(ev);
    }

    void Store(TEvEtcdKV::TEvTxnRequest::TPtr& ev) {
        CreateTables();
        StoredRequests.emplace_back(ev);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvEtcdKV::TEvCompactionRequest, Handle)
        hFunc(TEvEtcdKV::TEvCompactionResponse, Handle)

        hFunc(TEvEtcdKV::TEvDeleteRangeRequest, Handle)
        hFunc(TEvEtcdKV::TEvDeleteRangeResponse, Handle)

        hFunc(TEvEtcdKV::TEvPutRequest, Handle)
        hFunc(TEvEtcdKV::TEvPutResponse, Handle)

        hFunc(TEvEtcdKV::TEvRangeRequest, Handle)
        hFunc(TEvEtcdKV::TEvRangeResponse, Handle)

        hFunc(TEvEtcdKV::TEvTxnRequest, Handle)
        hFunc(TEvEtcdKV::TEvTxnResponse, Handle)
    )

    void Handle(TEvEtcdKV::TEvCompactionRequest::TPtr& ev) {
        Register(NYdb::NEtcd::CreateKVActor(kLogComponent, {}, Path, Cookie, std::move(ev->Get()->Request)));
        Requests[Cookie++] = ev->Sender;
    }

    void Handle(TEvEtcdKV::TEvCompactionResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (TEvEtcdKV::TEvCompactionResponse).");
            return;
        }
        Send(it->second, ev->Release());
        Requests.erase(it);
    }

    void Handle(TEvEtcdKV::TEvDeleteRangeRequest::TPtr& ev) {
        Register(NYdb::NEtcd::CreateKVActor(kLogComponent, {}, Path, Cookie, std::move(ev->Get()->Request)));
        Requests[Cookie++] = ev->Sender;
    }

    void Handle(TEvEtcdKV::TEvDeleteRangeResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (TEvEtcdKV::TEvDeleteRangeResponse).");
            return;
        }
        Send(it->second, ev->Release());
        Requests.erase(it);
    }

    void Handle(TEvEtcdKV::TEvPutRequest::TPtr& ev) {
        Register(NYdb::NEtcd::CreateKVActor(kLogComponent, {}, Path, Cookie, std::move(ev->Get()->Request)));
        Requests[Cookie++] = ev->Sender;
    }

    void Handle(TEvEtcdKV::TEvPutResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (TEvEtcdKV::TEvPutResponse).");
            return;
        }
        Send(it->second, ev->Release());
        Requests.erase(it);
    }

    void Handle(TEvEtcdKV::TEvRangeRequest::TPtr& ev) {
        Register(NYdb::NEtcd::CreateKVActor(kLogComponent, {}, Path, Cookie, std::move(ev->Get()->Request)));
        Requests[Cookie++] = ev->Sender;
    }

    void Handle(TEvEtcdKV::TEvRangeResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (TEvEtcdKV::TEvRangeResponse).");
            return;
        }
        Send(it->second, ev->Release());
        Requests.erase(it);
    }

    void Handle(TEvEtcdKV::TEvTxnRequest::TPtr& ev) {
        Register(NYdb::NEtcd::CreateKVActor(kLogComponent, {}, Path, Cookie, std::move(ev->Get()->Request)));
        Requests[Cookie++] = ev->Sender;
    }

    void Handle(TEvEtcdKV::TEvTxnResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (TEvEtcdKV::TEvTxnResponse).");
            return;
        }
        Send(it->second, ev->Release());
        Requests.erase(it);
    }

private:
    using TRequestPtr = std::variant<
        TEvEtcdKV::TEvRangeRequest::TPtr,
        TEvEtcdKV::TEvPutRequest::TPtr,
        TEvEtcdKV::TEvDeleteRangeRequest::TPtr,
        TEvEtcdKV::TEvTxnRequest::TPtr,
        TEvEtcdKV::TEvCompactionRequest::TPtr>;

    static constexpr auto kLogComponent = NKikimrServices::KQP_PROXY;
    const TString Path = "/Root/.etcd";
    ssize_t TablesCreating = 0;
    TVector<TRequestPtr> StoredRequests;
    ui64 Cookie = 0;
    TMap<ui64, NActors::TActorId> Requests;
};

NActors::IActor* CreateEtcdService() {
    return new TEtcdService();
}

} // namespace NYdb::NEtcd
