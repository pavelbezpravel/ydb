#include "kv_txn.h"

#include "events.h"
#include "proto.h"

#include "kv_delete.h"
#include "kv_put.h"
#include "kv_range.h"
#include "kv_txn_compare.h"

#include <utility>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NYdb::NEtcd {

namespace {

class TKVTxnActor : public NActors::TActorBootstrapped<TKVTxnActor> {
public:
    TKVTxnActor(ui64 logComponent, TString&& sessionId, TString&& path, NKikimr::TQueryBase::TTxControl txControl, TString&& txId, i64 revision, i64 compactRevision, TTxnRequest&& request)
        : LogComponent(logComponent)
        , SessionId(sessionId)
        , Path(path)
        , TxControl(txControl)
        , TxId(std::move(txId))
        , Revision(revision)
        , CompactRevision(compactRevision)
        , RequestIndex(-1)
        , Request(request) {
        LOG_D("[TKVTxnActor] TKVTxnActor::TKVTxnActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\" Request: " << Request);
    }

    void Bootstrap() {
        RunCompareQuery();
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TKVTxnActor>::Registered(sys, owner);
        Owner = owner;
    }

private:
    void RunCompareQuery() {
        Become(&TKVTxnActor::KVTxnCompareStateFunc);

        Register(CreateKVTxnCompareActor(LogComponent, SessionId, Path, TxControl, TxId, Revision, CompactRevision, Request.Compare, std::to_array({Request.Requests[0].size(), Request.Requests[1].size()})));
    }

    STRICT_STFUNC(KVTxnCompareStateFunc, hFunc(TEvEtcdKV::TEvTxnCompareResponse, Handle))
    void Handle(TEvEtcdKV::TEvTxnCompareResponse::TPtr& ev) {
        LOG_D("[TKVTxnActor] TKVTxnActor::Handle(TEvTxnCompareResponse) RequestIndex: " << RequestIndex << ", Response: " << ev->Get()->Response);
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        Response.Revision = Revision;
        Response.Succeeded = ev->Get()->Response.Succeeded;
        const TVector<TRequestOp>& Requests = Request.Requests[!Response.Succeeded];
        Response.Responses.reserve(Requests.size());

        LOG_D("[TKVTxnActor] TKVTxnActor::OnQueryResult(): Response: " << Response);

        RunQuery();
    }

    STRICT_STFUNC(KVDeleteRangeStateFunc, hFunc(TEvEtcdKV::TEvDeleteRangeResponse, Handle))
    void Handle(TEvEtcdKV::TEvDeleteRangeResponse::TPtr& ev) {
        LOG_D("[TKVTxnActor] TKVTxnActor::Handle(TEvDeleteRangeResponse) RequestIndex: " << RequestIndex << ", Response: " << ev->Get()->Response);
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        TxControl.Commit &= !ev->Get()->Response.IsWrite();

        SessionId = std::move(ev->Get()->SessionId);
        TxId = std::move(ev->Get()->TxId);
        Response.Responses.emplace_back(std::make_shared<TDeleteRangeResponse>(std::move(ev->Get()->Response)));

        RunQuery();
    }

    STRICT_STFUNC(KVPutStateFunc, hFunc(TEvEtcdKV::TEvPutResponse, Handle))
    void Handle(TEvEtcdKV::TEvPutResponse::TPtr& ev) {
        LOG_D("[TKVTxnActor] TKVTxnActor::Handle(TEvPutResponse) RequestIndex: " << RequestIndex << ", Response: " << ev->Get()->Response);
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        TxControl.Commit &= !ev->Get()->Response.IsWrite();

        SessionId = std::move(ev->Get()->SessionId);
        TxId = std::move(ev->Get()->TxId);
        Response.Responses.emplace_back(std::make_shared<TPutResponse>(std::move(ev->Get()->Response)));

        RunQuery();
    }

    STRICT_STFUNC(KVRangeStateFunc, hFunc(TEvEtcdKV::TEvRangeResponse, Handle))
    void Handle(TEvEtcdKV::TEvRangeResponse::TPtr& ev) {
        LOG_D("[TKVTxnActor] TKVTxnActor::Handle(TEvRangeResponse) RequestIndex: " << RequestIndex << ", Response: " << ev->Get()->Response);
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        TxControl.Commit &= !ev->Get()->Response.IsWrite();

        SessionId = std::move(ev->Get()->SessionId);
        TxId = std::move(ev->Get()->TxId);
        Response.Responses.emplace_back(std::make_shared<TRangeResponse>(std::move(ev->Get()->Response)));

        RunQuery();
    }

    STRICT_STFUNC(KVTxnStateFunc, hFunc(TEvEtcdKV::TEvTxnResponse, Handle))
    void Handle(TEvEtcdKV::TEvTxnResponse::TPtr& ev) {
        LOG_D("[TKVTxnActor] TKVTxnActor::Handle(TEvTxnResponse) RequestIndex: " << RequestIndex << ", Response: " << ev->Get()->Response);
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        TxControl.Commit &= !ev->Get()->Response.IsWrite();
        ev->Get()->Response.Revision = 0;

        SessionId = std::move(ev->Get()->SessionId);
        TxId = std::move(ev->Get()->TxId);
        Response.Responses.emplace_back(std::make_shared<TTxnResponse>(std::move(ev->Get()->Response)));

        RunQuery();
    }

    void RunQuery() {
        const TVector<TRequestOp>& Requests = Request.Requests[!Response.Succeeded];

        if (++RequestIndex == Requests.size()) {
            Finish();
            return;
        }
        auto currTxControl = [&]() {
            if (RequestIndex + 1 == Requests.size()) {
                return TxControl;
            } else {
                auto [currTxControl, nextTxControl] = TQueryBase::Split(TxControl);
                TxControl = nextTxControl;
                return currTxControl;
            }
        }();
        std::visit([&](const auto& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::shared_ptr<TDeleteRangeRequest>>) {
                Become(&TKVTxnActor::KVDeleteRangeStateFunc);
            } else if constexpr (std::is_same_v<T, std::shared_ptr<TPutRequest>>) {
                Become(&TKVTxnActor::KVPutStateFunc);
            } else if constexpr (std::is_same_v<T, std::shared_ptr<TRangeRequest>>) {
                Become(&TKVTxnActor::KVRangeStateFunc);
            } else if constexpr (std::is_same_v<T, std::shared_ptr<TTxnRequest>>) {
                Become(&TKVTxnActor::KVTxnStateFunc);
            } else {
                static_assert(sizeof(T) == 0);
            }
            Register(CreateKVQueryActor(LogComponent, SessionId, Path, currTxControl, TxId, Revision, CompactRevision, *arg));
        }, Requests[RequestIndex]);
    }

    void Finish() {
        Finish(Ydb::StatusIds::SUCCESS, NYql::TIssues());
    }

    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        LOG_D("[TKVTxnActor] TKVTxnActor::OnFinish(); Response: " << Response);
        Send(Owner, new TEvEtcdKV::TEvTxnResponse(status, std::move(issues), SessionId, TxId, std::move(Response)));
        PassAway();
    }

private:
    ui64 LogComponent;
    TString SessionId;
    TString Path;
    NKikimr::TQueryBase::TTxControl TxControl;
    TString TxId;
    NActors::TActorId Owner;

    i64 Revision;
    i64 CompactRevision;
    size_t RequestIndex;
    TTxnRequest Request;
    TTxnResponse Response;
};

} // anonymous namespace

NActors::IActor* CreateKVQueryActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, i64 revision, i64 compactRevision, TTxnRequest request) {
    return new TKVTxnActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, compactRevision, std::move(request));
}

} // namespace NYdb::NEtcd
