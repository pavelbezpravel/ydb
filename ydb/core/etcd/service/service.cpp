#include <ydb/core/etcd/kv/events.h>
#include <ydb/core/etcd/kv/kv_table_creator.h>
#include <ydb/core/etcd/kv/kv.h>

#include <ydb/core/etcd/revision/events.h>
#include <ydb/core/etcd/revision/revision_table_creator.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include "ydb/library/services/services.pb.h"

namespace NYdb::NEtcd {

class TEtcdService : public NActors::TActorBootstrapped<TEtcdService> {
public:
    void Bootstrap() {
        this->Become(&TEtcdService::StateProxy);
    }

private:
    STRICT_STFUNC(StateProxy,
        hFunc(TEvEtcdRevision::TEvCreateTableRequest, Handle)
        hFunc(TEvEtcdRevision::TEvRevisionResponse, Handle)

        hFunc(TEvEtcdKV::TEvCreateTableRequest, Handle)
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

    void Handle(TEvEtcdRevision::TEvCreateTableRequest::TPtr& ev) {
        std::cerr << "TEvEtcdRevision::TEvCreateTableRequest\n";
        this->Register(NYdb::NEtcd::CreateRevisionTableCreatorActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvEtcdRevision::TEvRevisionResponse::TPtr& ev) {
        std::cerr << "TEvEtcdRevision::TEvRevisionResponse\n";

        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request doesn't exist (TEvEtcdRevision::TEvRevisionResponse).\n";
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvEtcdRevision::TEvCreateTableRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request differs from the TEvEtcdRevision::TEvCreateTableRequest type.\n";
            return;
        }

        this->Send(this->SelfId(), new TEvEtcdKV::TEvCreateTableRequest(), {}, requestPtr->Get()->Cookie);
    }

    void Handle(TEvEtcdKV::TEvCreateTableRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvCreateTableRequest\n";
        this->Register(NYdb::NEtcd::CreateKVTableCreatorActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvEtcdKV::TEvCreateTableResponse::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvCreateTableResponse\n";
        
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request doesn't exist (TEvEtcdKV::TEvCreateTableResponse).\n";
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvEtcdKV::TEvCreateTableRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            // TODO [pavelbezpravel]: log error!
            std::cerr << "Request differs from the TEvEtcdKV::TEvCreateTableRequest type.\n";
            return;
        }

        HaveTablesCreated = true;
        InProgress = false;

        for (const auto& [cookie, request] : Requests) {
            std::visit([&](auto&& arg) {
                // TODO [pavelbezpravel]: Cookie, cookie or arg->Cookie?
                this->Send(new NActors::IEventHandle(this->SelfId(), arg->Sender, arg->Get(), {}, cookie));
            }, request);
        }
        Requests.clear();
    }

    void Handle(TEvEtcdKV::TEvRangeRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvRangeRequest\n";
        if (!HaveTablesCreated) {
            if (!InProgress) {
                InProgress = true;
                Requests[Cookie++] = ev;
                this->Send(this->SelfId(), new TEvEtcdRevision::TEvCreateTableRequest(), {}, Cookie);
                return;
            }
            Requests[Cookie++] = ev;
            return;
        }
        this->Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request_)));
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

        this->Send(requestPtr->Get()->Sender, ev->Get(), {}, requestPtr->Get()->Cookie);
    }

    void Handle(TEvEtcdKV::TEvPutRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvPutRequest\n";
        if (!HaveTablesCreated) {
            if (!InProgress) {
                InProgress = true;
                Requests[Cookie++] = ev;
                this->Send(this->SelfId(), new TEvEtcdRevision::TEvCreateTableRequest(), {}, Cookie);
                return;
            }
            Requests[Cookie++] = ev;
            return;
        }
        this->Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request_)));
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

        this->Send(requestPtr->Get()->Sender, ev->Get(), {}, requestPtr->Get()->Cookie);
    }

    void Handle(TEvEtcdKV::TEvDeleteRangeRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvDeleteRangeRequest\n";
        if (!HaveTablesCreated) {
            if (!InProgress) {
                InProgress = true;
                Requests[Cookie++] = ev;
                this->Send(this->SelfId(), new TEvEtcdRevision::TEvCreateTableRequest(), {}, Cookie);
                return;
            }
            Requests[Cookie++] = ev;
            return;
        }
        this->Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request_)));
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

        this->Send(requestPtr->Get()->Sender, ev->Get(), {}, requestPtr->Get()->Cookie);
    }

    void Handle(TEvEtcdKV::TEvTxnRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvTxnRequest\n";
        if (!HaveTablesCreated) {
            if (!InProgress) {
                InProgress = true;
                Requests[Cookie++] = ev;
                this->Send(this->SelfId(), new TEvEtcdRevision::TEvCreateTableRequest(), {}, Cookie);
                return;
            }
            Requests[Cookie++] = ev;
            return;
        }
        this->Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, Path, Cookie, std::move(ev->Get()->Request_)));
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

        this->Send(requestPtr->Get()->Sender, ev->Get(), {}, requestPtr->Get()->Cookie);
    }

    void Handle(TEvEtcdKV::TEvCompactionRequest::TPtr& ev) {
        std::cerr << "TEvEtcdKV::TEvCompactionRequest\n";
        if (!HaveTablesCreated) {
            if (!InProgress) {
                InProgress = true;
                Requests[Cookie++] = ev;
                this->Send(this->SelfId(), new TEvEtcdRevision::TEvCreateTableRequest(), {}, Cookie);
                return;
            }
            Requests[Cookie++] = ev;
            return;
        }
        // TODO [pavelbezpravel]: add actor for compaction.
        this->Send(ev->Sender, new TEvEtcdKV::TEvCompactionResponse({}, {}, {}, {}), {}, {});
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

        this->Send(requestPtr->Get()->Sender, ev->Get(), {}, requestPtr->Get()->Cookie);
    }

private:
    using TRequests = std::variant<
        TEvEtcdRevision::TEvCreateTableRequest::TPtr,
        TEvEtcdKV::TEvCreateTableRequest::TPtr,
        TEvEtcdKV::TEvRangeRequest::TPtr,
        TEvEtcdKV::TEvPutRequest::TPtr,
        TEvEtcdKV::TEvDeleteRangeRequest::TPtr,
        TEvEtcdKV::TEvTxnRequest::TPtr,
        TEvEtcdKV::TEvCompactionRequest::TPtr>;

    const TString Path = ".etcd";
    TMap<uint64_t, TRequests> Requests{};
    uint64_t Cookie = 0;
    bool HaveTablesCreated = false;
    bool InProgress = false;
};

NActors::IActor* CreateEtcdService() {
    return new TEtcdService();
}

} // namespace NYdb::NEtcd
