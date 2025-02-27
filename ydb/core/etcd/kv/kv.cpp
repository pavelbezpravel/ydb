#include "kv.h"

#include "events.h"
#include "proto.h"

#include "kv_compact.h"
#include "kv_delete.h"
#include "kv_put.h"
#include "kv_range.h"
#include "kv_txn.h"

#include <utility>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/core/etcd/revision/events.h>
#include <ydb/core/etcd/revision/revision_get.h>
#include <ydb/core/etcd/revision/revision_set.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/query_actor/query_actor.h>

namespace NYdb::NEtcd {

namespace {

void RevisionInc(TDeleteRangeResponse& resp) {
    ++resp.Revision;
}

void RevisionInc(TPutResponse& resp) {
    ++resp.Revision;
}

void RevisionInc(TRangeResponse& resp) {
    ++resp.Revision;
}

void RevisionInc(TTxnResponse& resp) {
    ++resp.Revision;
    for (auto& response : resp.Responses) {
        std::visit([](auto& arg) -> void {
            using T = std::decay_t<decltype(arg)>;
            RevisionInc(*arg);
            if constexpr (std::is_same_v<T, std::shared_ptr<TTxnResponse>>) {
                arg->Revision = 0;
            }
        }, response);
    }
}

template<typename TEvReq, typename TEvResp>
class TKVActor : public NActors::TActorBootstrapped<TKVActor<TEvReq, TEvResp>> {
    using TReq = decltype(std::declval<TEvReq>().Request);
    using TResp = decltype(std::declval<TEvResp>().Response);

public:
    TKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TReq request)
        : LogComponent(logComponent)
        , SessionId(sessionId)
        , Path(path)
        , TxControl(NKikimr::TQueryBase::TTxControl::BeginAndCommitTx())
        , Cookie(cookie)
        , Request(request) {
    }

    void Bootstrap() {
        auto [currTxControl, nextTxControl] = TQueryBase::Split(TxControl);
        TxControl = nextTxControl;

        RegisterRevisionGetRequest(currTxControl);
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TKVActor<TEvReq, TEvResp>>::Registered(sys, owner);
        Owner = owner;
    }

protected:
    void RegisterRevisionGetRequest(NKikimr::TQueryBase::TTxControl txControl) {
        this->Become(&TKVActor<TEvReq, TEvResp>::RevisionGetStateFunc);

        this->Register(CreateRevisionGetActor(LogComponent, SessionId, Path, txControl, TxId));
    }

    STRICT_STFUNC(RevisionGetStateFunc, hFunc(TEvEtcdRevision::TEvRevisionResponse, HandleRevisionGet))

    void HandleRevisionGet(TEvEtcdRevision::TEvRevisionResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        SessionId = std::move(ev->Get()->SessionId);
        TxId = std::move(ev->Get()->TxId);
        Revision = ev->Get()->Revision;
        CompactRevision = ev->Get()->CompactRevision;

        RegisterKVRequest(TxControl);
    }

    void RegisterKVRequest(NKikimr::TQueryBase::TTxControl txControl) {
        this->Become(&TKVActor<TEvReq, TEvResp>::KVStateFunc);

        this->Register(CreateKVQueryActor(LogComponent, SessionId, Path, txControl, TxId, Revision, CompactRevision, std::move(Request)));
    }

    STRICT_STFUNC(KVStateFunc, hFunc(TEvResp, Handle))

    void Handle(TEvResp::TPtr& ev) {
        LOG_D("[TKVBaseActor] TKVBaseActor::Handle(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\" Response: " << ev->Get()->Response);
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        SessionId = std::move(ev->Get()->SessionId);
        TxId = std::move(ev->Get()->TxId);
        Response = std::move(ev->Get()->Response);

        if (!Response.IsWrite()) {
            Finish();
            return;
        }

        if constexpr (std::is_same_v<TReq, TCompactionRequest>) {
            CompactRevision = Request.Revision;
        } else {
            RevisionInc(Response);
        }
        Revision = Response.Revision;

        RegisterRevisionSetRequest(TxControl, Revision, CompactRevision);
    }

    void RegisterRevisionSetRequest(NKikimr::TQueryBase::TTxControl txControl, i64 revision, i64 compactRevision) {
        this->Become(&TKVActor<TEvReq, TEvResp>::RevisionSetStateFunc);

        this->Register(CreateRevisionSetActor(LogComponent, SessionId, Path, txControl, TxId, revision, compactRevision));
    }

    STRICT_STFUNC(RevisionSetStateFunc, hFunc(TEvEtcdRevision::TEvRevisionResponse, HandleRevisionSet))

    void HandleRevisionSet(TEvEtcdRevision::TEvRevisionResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        SessionId = std::move(ev->Get()->SessionId);
        TxId = std::move(ev->Get()->TxId);
        Revision = ev->Get()->Revision;
        CompactRevision = ev->Get()->CompactRevision;

        Finish();
    }

    void Finish() {
        Finish(Ydb::StatusIds::SUCCESS, NYql::TIssues());
    }

    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        this->Send(Owner, new TEvResp(status, std::move(issues), SessionId, TxId, std::move(Response)), {}, Cookie);
        this->PassAway();
    }

protected:
    ui64 LogComponent;
    TString SessionId;
    TString Path;
    NKikimr::TQueryBase::TTxControl TxControl;
    TString TxId;
    NActors::TActorId Owner;

    ui64 Cookie;
    i64 Revision;
    i64 CompactRevision;
    TReq Request;
    TResp Response;
};

class TKVBatchActor : public NActors::TActorBootstrapped<TKVBatchActor> {
public:
    TKVBatchActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TVector<TPutRequest> request)
        : LogComponent(logComponent)
        , SessionId(sessionId)
        , Path(path)
        , Cookie(cookie)
        , Requests(request) {
        LOG_D("[TKVBatchActor] TKVBatchActor::TKVBatchActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" len(Requests): " << Requests.size());
    }

    void Bootstrap() {
        RegisterRevisionGetRequest();
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TKVBatchActor>::Registered(sys, owner);
        Owner = owner;
    }

protected:
    void RegisterRevisionGetRequest() {
        this->Become(&TKVBatchActor::RevisionGetStateFunc);

        this->Register(CreateRevisionGetActor(LogComponent, SessionId, Path, NKikimr::TQueryBase::TTxControl::BeginTx(), TxId));
    }

    STRICT_STFUNC(RevisionGetStateFunc, hFunc(TEvEtcdRevision::TEvRevisionResponse, HandleRevisionGet))

    void HandleRevisionGet(TEvEtcdRevision::TEvRevisionResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        SessionId = std::move(ev->Get()->SessionId);
        TxId = std::move(ev->Get()->TxId);
        Revision = ev->Get()->Revision;
        CompactRevision = ev->Get()->CompactRevision;

        RegisterKVRequest();
    }

    void RegisterKVRequest() {
        if (Responses.size() >= Requests.size()) {
            RegisterRevisionSetRequest();
            return;
        }

        this->Become(&TKVBatchActor::KVStateFunc);

        this->Register(CreateKVQueryActor(LogComponent, SessionId, Path, NKikimr::TQueryBase::TTxControl::ContinueTx(), TxId, Revision, CompactRevision, std::move(Requests[Responses.size()])));
    }

    STRICT_STFUNC(KVStateFunc, hFunc(TEvEtcdKV::TEvPutResponse, Handle))

    void Handle(TEvEtcdKV::TEvPutResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        SessionId = std::move(ev->Get()->SessionId);
        TxId = std::move(ev->Get()->TxId);
        Responses.emplace_back(std::move(ev->Get()->Response));

        RevisionInc(Responses.back());
        Revision = Responses.back().Revision;

        RegisterKVRequest();
    }

    void RegisterRevisionSetRequest() {
        this->Become(&TKVBatchActor::RevisionSetStateFunc);

        this->Register(CreateRevisionSetActor(LogComponent, SessionId, Path, NKikimr::TQueryBase::TTxControl::ContinueAndCommitTx(), TxId, Revision, CompactRevision));
    }

    STRICT_STFUNC(RevisionSetStateFunc, hFunc(TEvEtcdRevision::TEvRevisionResponse, HandleRevisionSet))

    void HandleRevisionSet(TEvEtcdRevision::TEvRevisionResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        SessionId = std::move(ev->Get()->SessionId);
        TxId = std::move(ev->Get()->TxId);
        Revision = ev->Get()->Revision;
        CompactRevision = ev->Get()->CompactRevision;

        Finish();
    }

    void Finish() {
        Finish(Ydb::StatusIds::SUCCESS, NYql::TIssues());
    }

    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        LOG_D("[TKVBatchActor] TKVBatchActor::Finish();");
        this->Send(Owner, new TEvEtcdKV::TEvResponse(status, std::move(issues), SessionId, TxId, std::move(Responses)), {}, Cookie);
        this->PassAway();
    }

protected:
    ui64 LogComponent;
    TString SessionId;
    TString Path;
    TString TxId;
    NActors::TActorId Owner;

    ui64 Cookie;
    i64 Revision;
    i64 CompactRevision;
    TVector<TPutRequest> Requests;
    TVector<TPutResponse> Responses;
};

} // anonymous namespace

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TCompactionRequest request) {
    using TRetryActor = NKikimr::TQueryRetryActor<TKVActor<TEvEtcdKV::TEvCompactionRequest, TEvEtcdKV::TEvCompactionResponse>, TEvEtcdKV::TEvCompactionResponse, ui64, TString, TString, ui64, TCompactionRequest>;
    
    return new TRetryActor(
        NActors::TActivationContext::AsActorContext().SelfID,
        TRetryActor::IRetryPolicy::GetExponentialBackoffPolicy(TRetryActor::Retryable, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(1), std::numeric_limits<size_t>::max(), TDuration::Minutes(5)),
        logComponent, std::move(sessionId), std::move(path), cookie, std::move(request)
    );
    
    // return new TKVActor<TEvEtcdKV::TEvCompactionRequest, TEvEtcdKV::TEvCompactionResponse>(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TDeleteRangeRequest request) {
    using TRetryActor = NKikimr::TQueryRetryActor<TKVActor<TEvEtcdKV::TEvDeleteRangeRequest, TEvEtcdKV::TEvDeleteRangeResponse>, TEvEtcdKV::TEvDeleteRangeResponse, ui64, TString, TString, ui64, TDeleteRangeRequest>;
    
    return new TRetryActor(
        NActors::TActivationContext::AsActorContext().SelfID,
        TRetryActor::IRetryPolicy::GetExponentialBackoffPolicy(TRetryActor::Retryable, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(1), std::numeric_limits<size_t>::max(), TDuration::Minutes(5)),
        logComponent, std::move(sessionId), std::move(path), cookie, std::move(request)
    );
    
    // return new TKVActor<TEvEtcdKV::TEvDeleteRangeRequest, TEvEtcdKV::TEvDeleteRangeResponse>(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TPutRequest request) {
    using TRetryActor = NKikimr::TQueryRetryActor<TKVActor<TEvEtcdKV::TEvPutRequest, TEvEtcdKV::TEvPutResponse>, TEvEtcdKV::TEvPutResponse, ui64, TString, TString, ui64, TPutRequest>;
    
    return new TRetryActor(
        NActors::TActivationContext::AsActorContext().SelfID,
        TRetryActor::IRetryPolicy::GetExponentialBackoffPolicy(TRetryActor::Retryable, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(1), std::numeric_limits<size_t>::max(), TDuration::Minutes(5)),
        logComponent, std::move(sessionId), std::move(path), cookie, std::move(request)
    );
    
    // return new TKVActor<TEvEtcdKV::TEvPutRequest, TEvEtcdKV::TEvPutResponse>(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TRangeRequest request) {
    using TRetryActor = NKikimr::TQueryRetryActor<TKVActor<TEvEtcdKV::TEvRangeRequest, TEvEtcdKV::TEvRangeResponse>, TEvEtcdKV::TEvRangeResponse, ui64, TString, TString, ui64, TRangeRequest>;
    
    return new TRetryActor(
        NActors::TActivationContext::AsActorContext().SelfID,
        TRetryActor::IRetryPolicy::GetExponentialBackoffPolicy(TRetryActor::Retryable, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(1), std::numeric_limits<size_t>::max(), TDuration::Minutes(5)),
        logComponent, std::move(sessionId), std::move(path), cookie, std::move(request)
    );
    
    // return new TKVActor<TEvEtcdKV::TEvRangeRequest, TEvEtcdKV::TEvRangeResponse>(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TTxnRequest request) {
    using TRetryActor = NKikimr::TQueryRetryActor<TKVActor<TEvEtcdKV::TEvTxnRequest, TEvEtcdKV::TEvTxnResponse>, TEvEtcdKV::TEvTxnResponse, ui64, TString, TString, ui64, TTxnRequest>;
    
    return new TRetryActor(
        NActors::TActivationContext::AsActorContext().SelfID,
        TRetryActor::IRetryPolicy::GetExponentialBackoffPolicy(TRetryActor::Retryable, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(1), std::numeric_limits<size_t>::max(), TDuration::Minutes(5)),
        logComponent, std::move(sessionId), std::move(path), cookie, std::move(request)
    );
    
    // return new TKVActor<TEvEtcdKV::TEvTxnRequest, TEvEtcdKV::TEvTxnResponse>(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TVector<TPutRequest> request) {
    using TRetryActor = NKikimr::TQueryRetryActor<TKVBatchActor, TEvEtcdKV::TEvResponse, ui64, TString, TString, ui64, TVector<TPutRequest>>;

    return new TRetryActor(
        NActors::TActivationContext::AsActorContext().SelfID,
        TRetryActor::IRetryPolicy::GetExponentialBackoffPolicy(TRetryActor::Retryable, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(1), std::numeric_limits<size_t>::max(), TDuration::Minutes(5)),
        logComponent, std::move(sessionId), std::move(path), cookie, std::move(request)
    );
}

} // namespace NYdb::NEtcd
