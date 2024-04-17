#include "ydb/core/etcd/service/service.h"

#include <ydb/core/etcd/kv/events.h>
#include <ydb/core/etcd/kv/kv_table_create.h>
#include <ydb/core/etcd/kv/kv.h>

#include <ydb/core/etcd/revision/events.h>
#include <ydb/core/etcd/revision/revision_table_create.h>
#include <ydb/core/etcd/revision/revision_table_init.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/table_creator/table_creator.h>
#include "ydb/library/services/services.pb.h"

#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_PROXY, "[ydb] [EtcdService]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_PROXY, "[ydb] [EtcdService]: " << stream)

namespace NYdb::NEtcd {

class TEtcdService : public NActors::TActorBootstrapped<TEtcdService> {
public:
    void Bootstrap() {
        Become(&TEtcdService::StateProxy);
    }

private:
    STRICT_STFUNC(StateProxy,
        hFunc(TEvEtcdRevision::TEvCreateTableResponse, Handle)
        hFunc(TEvEtcdRevision::TEvRevisionResponse, Handle)
        hFunc(TEvEtcdKV::TEvCreateTableResponse, Handle)

        hFunc(TEvEtcdKV::TEvRangeRequest, Handle)
        hFunc(TEvEtcdKV::TEvRangeResponse, Handle)

        hFunc(TEvEtcdKV::TEvPutRequest, Handle)
        hFunc(TEvEtcdKV::TEvPutResponse, Handle)

        hFunc(TEvEtcdKV::TEvDeleteRangeRequest, Handle)
        hFunc(TEvEtcdKV::TEvDeleteRangeResponse, Handle)

        hFunc(TEvEtcdKV::TEvTxnRequest, Handle)
        hFunc(TEvEtcdKV::TEvTxnResponse, Handle)

        hFunc(TEvEtcdKV::TEvCompactionRequest, Handle)
        hFunc(TEvEtcdKV::TEvCompactionResponse, Handle)
    )

    void CreateTables() {
        CreateRevisionTable();
        CreateKVTable();
    }

    void CreateRevisionTable() {
        std::cerr << "TEvEtcdRevision::TEvCreateTableRequest\n";
        Register(NYdb::NEtcd::CreateRevisionTableCreateActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie++));
    }

    void Handle(TEvEtcdRevision::TEvCreateTableResponse::TPtr& ev) {
        std::cerr << "TEvEtcdRevision::TEvCreateTableResponse\n";
        LOG_D(ev->ToString());
        Register(NYdb::NEtcd::CreateRevisionTableInitActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie++));
    }

    void Handle(TEvEtcdRevision::TEvRevisionResponse::TPtr& ev) {
        std::cerr << "TEvEtcdRevision::TEvRevisionResponse\n";
        LOG_D(ev->ToString());
        ProcessCached();
    }

    void CreateKVTable() {
        std::cerr << "TEvEtcdKV::TEvCreateTableRequest\n";
        Register(NYdb::NEtcd::CreateKVTableCreateActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie++));
    }

    void Handle(TEvEtcdKV::TEvCreateTableResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvCreateTableResponse\n";
        LOG_D(ev->ToString());
        ProcessCached();
    }

    void ProcessCached() {
        Y_ABORT_UNLESS(TablesCreating > 0);
        if (--TablesCreating == 0) {
            for (const auto& [cookie, request] : Requests) {
                std::visit([&](auto&& arg) {
                    Send(new NActors::IEventHandle(SelfId(), arg->Sender, arg->Get()));
                }, request);
            }
            // TODO [pavelbezpravel]: We need to clear Requests, I guess, but it works.
            // Requests.clear();
        }
    }

    void Handle(TEvEtcdKV::TEvRangeRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvRangeRequest\n";
        if (TablesCreating != 0) {
            Requests[Cookie++] = ev;
            if (!InProgress) {
                InProgress = true;
                CreateTables();
            }
            return;
        }
        Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request_)));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvEtcdKV::TEvRangeResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvRangeResponse\n";

        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request doesn't exist (TEvEtcdKV::TEvRangeResponse).\n";
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvEtcdKV::TEvRangeRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request differs from the TEvEtcdKV::TEvRangeRequest type.\n";
            return;
        }

        Send(requestPtr->Get()->Sender, ev->Release());
    }

    void Handle(TEvEtcdKV::TEvPutRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvPutRequest\n";
        if (TablesCreating != 0) {
            Requests[Cookie++] = ev;
            if (!InProgress) {
                InProgress = true;
                CreateTables();
            }
            return;
        }
        Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request_)));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvEtcdKV::TEvPutResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvPutResponse\n";

        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request doesn't exist (TEvEtcdKV::TEvPutResponse).\n";
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvEtcdKV::TEvPutRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request differs from the TEvEtcdKV::TEvPutRequest type.\n";
            return;
        }

        Send(requestPtr->Get()->Sender, ev->Release());
    }

    void Handle(TEvEtcdKV::TEvDeleteRangeRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvDeleteRangeRequest\n";
        if (TablesCreating != 0) {
            Requests[Cookie++] = ev;
            if (!InProgress) {
                InProgress = true;
                CreateTables();
            }
            return;
        }
        Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request_)));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvEtcdKV::TEvDeleteRangeResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvDeleteRangeResponse\n";

        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request doesn't exist (TEvEtcdKV::TEvDeleteRangeResponse).\n";
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvEtcdKV::TEvDeleteRangeRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request differs from the TEvEtcdKV::TEvDeleteRangeRequest type.\n";
            return;
        }

        Send(requestPtr->Get()->Sender, ev->Release());
    }

    void Handle(TEvEtcdKV::TEvTxnRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvTxnRequest\n";
        if (TablesCreating != 0) {
            Requests[Cookie++] = ev;
            if (!InProgress) {
                InProgress = true;
                CreateTables();
            }
            return;
        }
        Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request_)));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvEtcdKV::TEvTxnResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvTxnResponse\n";

        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request doesn't exist (TEvEtcdKV::TEvTxnResponse).\n";
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvEtcdKV::TEvTxnRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request differs from the TEvEtcdKV::TEvTxnRequest type.\n";
            return;
        }

        Send(requestPtr->Get()->Sender, ev->Release());
    }

    void Handle(TEvEtcdKV::TEvCompactionRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvCompactionRequest\n";
        if (TablesCreating != 0) {
            Requests[Cookie++] = ev;
            if (!InProgress) {
                InProgress = true;
                CreateTables();
            }
            return;
        }
        // TODO [pavelbezpravel]: add actor for compaction.
        Send(ev->Sender, new TEvEtcdKV::TEvCompactionResponse({}, {}, {}, {}), {}, {});
        Requests[Cookie++] = ev;
    }

    void Handle(TEvEtcdKV::TEvCompactionResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvCompactionResponse\n";

       auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request doesn't exist (TEvEtcdKV::TEvCompactionResponse).\n";
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvEtcdKV::TEvCompactionRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request differs from the TEvEtcdKV::TEvCompactionRequest type.\n";
            return;
        }

        Send(requestPtr->Get()->Sender, ev->Release());
    }

private:
    using TRequests = std::variant<
        TEvEtcdKV::TEvRangeRequest::TPtr,
        TEvEtcdKV::TEvPutRequest::TPtr,
        TEvEtcdKV::TEvDeleteRangeRequest::TPtr,
        TEvEtcdKV::TEvTxnRequest::TPtr,
        TEvEtcdKV::TEvCompactionRequest::TPtr>;

    const TString Path = ".etcd";
    TMap<uint64_t, TRequests> Requests{};
    uint64_t Cookie = 0;
    bool InProgress = false;
    size_t TablesCreating = 2;
};

NActors::IActor* CreateEtcdService() {
    return new TEtcdService();
}

} // namespace NYdb::NEtcd
