#include "ydb/core/etcd/service/service.h"

#include <util/generic/queue.h>

#include <ydb/core/etcd/kv/events.h>
#include <ydb/core/etcd/kv/kv_table_create.h>
#include <ydb/core/etcd/kv/kv.h>

#include <ydb/core/etcd/revision/events.h>
#include <ydb/core/etcd/revision/revision_table_create.h>

#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/library/table_creator/table_creator.h>
#include "ydb/library/services/services.pb.h"

#define LOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, kLogComponent, "[ydb] [ETCD_FEATURE] [EtcdService]: " << stream)

namespace NYdb::NEtcd {

class TEtcdService : public NActors::TActorBootstrapped<TEtcdService> {
public:
    explicit TEtcdService(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig)
        : MaxSessionCount(queryServiceConfig.GetMaxSessionCount()) {
    }

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
            LOG_W("Cannot create EtcdService, Issues: " << ev->Get()->Issues.ToOneLineString());
            return;
        }
        SessionIds.push(std::move(ev->Get()->SessionId));
        HandleCreateTables();
    }

    void CreateKVTable() {
        ++TablesCreating;
        Register(NYdb::NEtcd::CreateKVTableCreateActor(kLogComponent, Path));
    }

    void Handle(TEvEtcdKV::TEvCreateTableResponse::TPtr& ev) {
        Y_UNUSED(ev);
        HandleCreateTables();
    }

    void HandleCreateTables() {
        Y_ABORT_UNLESS(TablesCreating > 0);
        if (--TablesCreating > 0) {
            return;
        }
        Become(&TEtcdService::StateFunc);
        ProcessStored();
    }

    void ProcessStored() {
        if (!StoredRequests.empty() && std::holds_alternative<TEvEtcdKV::TEvPutRequest::TPtr>(StoredRequests.front())) {
            if (!Requests.empty()) {
                return;
            }
            RunningWriteReq = true;
            auto sessionId = TString{};
            if (!SessionIds.empty()) {
                sessionId = SessionIds.front();
                SessionIds.pop();
            }
            TVector<TPutRequest> requests;
            while (!StoredRequests.empty() && requests.size() < MaxSessionCount && std::holds_alternative<TEvEtcdKV::TEvPutRequest::TPtr>(StoredRequests.front())) {
                auto& req = std::get<TEvEtcdKV::TEvPutRequest::TPtr>(StoredRequests.front());
                requests.emplace_back(std::move(req->Get()->Request));
                Requests[Cookie++] = std::move(req->Sender);
                StoredRequests.pop();
            }
            Register(NYdb::NEtcd::CreateKVActor(kLogComponent, sessionId, Path, Cookie, std::move(requests)));
            return;
        }

        while (!StoredRequests.empty() && std::visit([&](const auto& ev) { return SendRequest(ev); }, StoredRequests.front())) {
            StoredRequests.pop();
        }
    }

    void Store(TEvEtcdKV::TEvCompactionRequest::TPtr& ev) {
        CreateTables();
        StoredRequests.emplace(ev);
    }

    void Store(TEvEtcdKV::TEvDeleteRangeRequest::TPtr& ev) {
        CreateTables();
        StoredRequests.emplace(ev);
    }

    void Store(TEvEtcdKV::TEvPutRequest::TPtr& ev) {
        CreateTables();
        StoredRequests.emplace(ev);
    }

    void Store(TEvEtcdKV::TEvRangeRequest::TPtr& ev) {
        CreateTables();
        StoredRequests.emplace(ev);
    }

    void Store(TEvEtcdKV::TEvTxnRequest::TPtr& ev) {
        CreateTables();
        StoredRequests.emplace(ev);
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

        hFunc(TEvEtcdKV::TEvResponse, Handle)
    )

    void Handle(TEvEtcdKV::TEvCompactionRequest::TPtr& ev) {
        HandleRequest(ev);
    }

    void Handle(TEvEtcdKV::TEvCompactionResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_W("Request doesn't exist (TEvEtcdKV::TEvCompactionResponse).");
            return;
        }
        SessionIds.push(std::move(ev->Get()->SessionId));
        Send(it->second, ev->Release());
        Requests.erase(it);
        ProcessStored();
    }

    void Handle(TEvEtcdKV::TEvDeleteRangeRequest::TPtr& ev) {
        HandleRequest(ev);
    }

    void Handle(TEvEtcdKV::TEvDeleteRangeResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_W("Request doesn't exist (TEvEtcdKV::TEvDeleteRangeResponse).");
            return;
        }
        SessionIds.push(std::move(ev->Get()->SessionId));
        Send(it->second, ev->Release());
        Requests.erase(it);
        ProcessStored();
    }

    void Handle(TEvEtcdKV::TEvPutRequest::TPtr& ev) {
        HandleRequest(ev);
    }

    void Handle(TEvEtcdKV::TEvPutResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_W("Request doesn't exist (TEvEtcdKV::TEvPutResponse).");
            return;
        }
        SessionIds.push(std::move(ev->Get()->SessionId));
        Send(it->second, ev->Release());
        Requests.erase(it);
        ProcessStored();
    }

    void Handle(TEvEtcdKV::TEvRangeRequest::TPtr& ev) {
        HandleRequest(ev);
    }

    void Handle(TEvEtcdKV::TEvRangeResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_W("Request doesn't exist (TEvEtcdKV::TEvRangeResponse).");
            return;
        }
        SessionIds.push(std::move(ev->Get()->SessionId));
        Send(it->second, ev->Release());
        Requests.erase(it);
        ProcessStored();
    }

    void Handle(TEvEtcdKV::TEvTxnRequest::TPtr& ev) {
        HandleRequest(ev);
    }

    void Handle(TEvEtcdKV::TEvTxnResponse::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_W("Request doesn't exist (TEvEtcdKV::TEvTxnResponse).");
            return;
        }
        SessionIds.push(std::move(ev->Get()->SessionId));
        Send(it->second, ev->Release());
        Requests.erase(it);
        ProcessStored();
    }

    void Handle(TEvEtcdKV::TEvResponse::TPtr& ev) {
        for (ui64 i = 0; i < ev->Get()->Responses.size(); ++i) {
            auto it = Requests.find(ev->Cookie - ev->Get()->Responses.size() + i);
            if (it == Requests.end()) {
                LOG_W("Request doesn't exist (TEvEtcdKV::TEvPutResponse).");
                continue;
            }
            auto copy = ev->Get()->Issues;
            Send(it->second, new TEvEtcdKV::TEvPutResponse(ev->Get()->Status, std::move(copy), {}, {}, std::move(ev->Get()->Responses[i])));
            Requests.erase(it);
        }
        SessionIds.push(std::move(ev->Get()->SessionId));
        ProcessStored();
    }

    template<typename TEventType>
    void HandleRequest(const TAutoPtr<NActors::TEventHandle<TEventType>>& ev) {
        if (!StoredRequests.empty() || !SendRequest(ev)) {
            StoredRequests.emplace(ev);
        }
    }

    template<typename TEventType>
    [[nodiscard]] bool SendRequest(const TAutoPtr<NActors::TEventHandle<TEventType>>& ev) {
        if (Requests.size() >= MaxSessionCount) {
            return false;
        }
        if ((ev->Get()->Request.IsWrite() && !Requests.empty()) || (!ev->Get()->Request.IsWrite() && !Requests.empty() && RunningWriteReq)) {
            return false;
        }
        RunningWriteReq = ev->Get()->Request.IsWrite();
        auto sessionId = TString{};
        if (!SessionIds.empty()) {
            sessionId = SessionIds.front();
            SessionIds.pop();
        }
        Register(NYdb::NEtcd::CreateKVActor(kLogComponent, sessionId, Path, Cookie, std::move(ev->Get()->Request)));
        Requests[Cookie++] = std::move(ev->Sender);
        return true;
    }

private:
    using TRequestPtr = std::variant<
        TEvEtcdKV::TEvRangeRequest::TPtr,
        TEvEtcdKV::TEvPutRequest::TPtr,
        TEvEtcdKV::TEvDeleteRangeRequest::TPtr,
        TEvEtcdKV::TEvTxnRequest::TPtr,
        TEvEtcdKV::TEvCompactionRequest::TPtr>;

    static constexpr auto kLogComponent = NKikimrServices::KQP_PROXY;
    ui64 MaxSessionCount;
    const TString Path = "/Root/.etcd";
    ssize_t TablesCreating = 0;
    TQueue<TRequestPtr> StoredRequests;
    TQueue<TString> SessionIds;
    bool RunningWriteReq = false;
    ui64 Cookie = 0;
    TMap<ui64, NActors::TActorId> Requests;
};

NActors::IActor* CreateEtcdService(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig) {
    return new TEtcdService(queryServiceConfig);
}

} // namespace NYdb::NEtcd
