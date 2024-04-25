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

#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_PROXY, "[ydb] [ETCD_FEATURE] [EtcdService]: " << stream)

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
        Register(NYdb::NEtcd::CreateRevisionTableCreateActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie++));
    }

    void Handle(TEvEtcdRevision::TEvCreateTableResponse::TPtr&) {
        Register(NYdb::NEtcd::CreateRevisionTableInitActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie++));
    }

    void Handle(TEvEtcdRevision::TEvRevisionResponse::TPtr&) {
        ProcessCached();
    }

    void CreateKVTable() {
        Register(NYdb::NEtcd::CreateKVTableCreateActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie++));
    }

    void Handle(TEvEtcdKV::TEvCreateTableResponse::TPtr&) {
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
        if (TablesCreating != 0) {
            Requests[Cookie++] = ev;
            if (!InProgress) {
                InProgress = true;
                CreateTables();
            }
            return;
        }
        Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request), std::exchange(IsFirstRequest, false)));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvEtcdKV::TEvRangeResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (TEvEtcdKV::TEvRangeResponse).");
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvEtcdKV::TEvRangeRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            LOG_E("Request differs from the TEvEtcdKV::TEvRangeRequest type.");
            return;
        }
        Send(requestPtr->Get()->Sender, ev->Release());
    }

    void Handle(TEvEtcdKV::TEvPutRequest::TPtr& ev) {
        if (TablesCreating != 0) {
            Requests[Cookie++] = ev;
            if (!InProgress) {
                InProgress = true;
                CreateTables();
            }
            return;
        }
        Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request), std::exchange(IsFirstRequest, false)));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvEtcdKV::TEvPutResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (TEvEtcdKV::TEvPutResponse).");
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvEtcdKV::TEvPutRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            LOG_E("Request differs from the TEvEtcdKV::TEvPutRequest type.");
            return;
        }
        Send(requestPtr->Get()->Sender, ev->Release());
    }

    void Handle(TEvEtcdKV::TEvDeleteRangeRequest::TPtr& ev) {
        if (TablesCreating != 0) {
            Requests[Cookie++] = ev;
            if (!InProgress) {
                InProgress = true;
                CreateTables();
            }
            return;
        }
        Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request), std::exchange(IsFirstRequest, false)));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvEtcdKV::TEvDeleteRangeResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (TEvEtcdKV::TEvDeleteRangeResponse).");
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvEtcdKV::TEvDeleteRangeRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            LOG_E("Request differs from the TEvEtcdKV::TEvDeleteRangeRequest type.");
            return;
        }
        Send(requestPtr->Get()->Sender, ev->Release());
    }

    void Handle(TEvEtcdKV::TEvTxnRequest::TPtr& ev) {
        if (TablesCreating != 0) {
            Requests[Cookie++] = ev;
            if (!InProgress) {
                InProgress = true;
                CreateTables();
            }
            return;
        }
        Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request), std::exchange(IsFirstRequest, false)));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvEtcdKV::TEvTxnResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (TEvEtcdKV::TEvTxnResponse).");
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvEtcdKV::TEvTxnRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            LOG_E("Request differs from the TEvEtcdKV::TEvTxnRequest type.");
            return;
        }
        Send(requestPtr->Get()->Sender, ev->Release());
    }

    void Handle(TEvEtcdKV::TEvCompactionRequest::TPtr& ev) {
        if (TablesCreating != 0) {
            Requests[Cookie++] = ev;
            if (!InProgress) {
                InProgress = true;
                CreateTables();
            }
            return;
        }
        Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request), std::exchange(IsFirstRequest, false)));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvEtcdKV::TEvCompactionResponse::TPtr& ev) {
       auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (TEvEtcdKV::TEvCompactionResponse).");
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvEtcdKV::TEvCompactionRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            LOG_E("Request differs from the TEvEtcdKV::TEvCompactionRequest type.");
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

    const TString Path = "/Root/.etcd";
    TMap<ui64, TRequests> Requests{};
    ui64 Cookie = 0;
    bool InProgress = false;
    bool IsFirstRequest = true;
    size_t TablesCreating = 2;
};

NActors::IActor* CreateEtcdService() {
    return new TEtcdService();
}

} // namespace NYdb::NEtcd
