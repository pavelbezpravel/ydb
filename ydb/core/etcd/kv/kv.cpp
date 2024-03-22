#include "kv.h"

#include <utility>
#include <ydb/core/etcd/revision/events.h>
#include <ydb/core/etcd/revision/revision_inc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/etcd/revision/query_base.h>
#include "kv_delete.h"
#include "kv_put.h"
#include "kv_range.h"
#include "kv_txn.h"
#include "events.h"

namespace NYdb::NEtcd {

namespace {

class TKvDeleteActor;
class TKvPutActor;
class TKvRangeActor;
class TKvTxnActor;

template<typename TActor>
struct TResponseTraits;

template<>
struct TResponseTraits<TKvDeleteActor> {
    using type = TEvEtcdKv::TEvDeleteResponse;
};

template<>
struct TResponseTraits<TKvPutActor> {
    using type = TEvEtcdKv::TEvPutResponse;
};

template<>
struct TResponseTraits<TKvRangeActor> {
    using type = TEvEtcdKv::TEvRangeResponse;
};

template<>
struct TResponseTraits<TKvTxnActor> {
    using type = TEvEtcdKv::TEvTxnResponse;
};

template<typename TDerived>
class TKvBaseActor : public NActors::TActorBootstrapped<TDerived> {
public:
    TKvBaseActor(ui64 logComponent, TString&& sessionId, TString&& path)
        : LogComponent(logComponent)
        , SessionId(sessionId)
        , Path(path) {
        TxControl = NKikimr::TQueryBase::TTxControl::BeginAndCommitTx();
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TKvBaseActor<TDerived>>::Registered(sys, owner);
        Owner = owner;
    }

protected:
    using TEvKvResponse = typename TResponseTraits<TDerived>::type;

    TDerived& GetDerived() {
        return static_cast<TDerived&>(*this);
    }

    void RegisterRevisionIncRequest() {
        auto [currTxControl, nextTxControl] = TQueryBase::Split(TxControl);
        TxControl = nextTxControl;

        this->Become(&TKvBaseActor<TDerived>::RevisionStateFunc);

        this->Register(CreateRevisionIncActor(LogComponent, SessionId, Path, TxControl));
    }

    STRICT_STFUNC(RevisionStateFunc, hFunc(TEvEtcdRevision::TEvRevisionResponse, Handle))
    void Handle(TEvEtcdRevision::TEvRevisionResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
        }

        TxId = std::move(ev->Get()->TxId);
        Revision = ev->Get()->Revision;
        
        GetDerived().RegisterKvRequest();
    }

    STRICT_STFUNC(KvStateFunc, hFunc(TEvKvResponse, Handle))

    void Handle(TEvKvResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
        }

        GetDerived().Response = std::move(ev->Get()->Response);

        Finish();
    }

    void Finish() {
        Finish(Ydb::StatusIds::SUCCESS, NYql::TIssues());
    }

    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        this->Send(Owner, new TEvKvResponse(status, std::move(issues), TxId, std::move(GetDerived().Response)));
        this->PassAway();
    }

protected:
    ui64 LogComponent;
    TString SessionId;
    TString TxId;
    NActors::TActorId Owner;

    TString Path;
    NKikimr::TQueryBase::TTxControl TxControl;
    
    i64 Revision;
};

class TKvDeleteActor : public TKvBaseActor<TKvDeleteActor> {
public:
    TKvDeleteActor(ui64 logComponent, TString&& sessionId, TString&& path, TDeleteRequest&& request)
        : TKvBaseActor<TKvDeleteActor>(logComponent, std::move(sessionId), std::move(path))
        , Request(request) {
    }

    void Bootstrap() {
        RegisterRevisionIncRequest();
    }

    void RegisterKvRequest() {
        Become(&TKvDeleteActor::KvStateFunc);

        Register(CreateKvDeleteActor(LogComponent, SessionId, Path, TxControl, Revision, std::move(Request)));
    }

public:
    TDeleteRequest Request;
    TDeleteResponse Response;
};

class TKvPutActor : public TKvBaseActor<TKvPutActor> {
public:
    TKvPutActor(ui64 logComponent, TString&& sessionId, TString&& path, TPutRequest&& request)
        : TKvBaseActor<TKvPutActor>(logComponent, std::move(sessionId), std::move(path))
        , Request(request) {
    }

    void Bootstrap() {
        RegisterRevisionIncRequest();
    }

    void RegisterKvRequest() {
        Become(&TKvPutActor::KvStateFunc);

        Register(CreateKvPutActor(LogComponent, SessionId, Path, TxControl, Revision, std::move(Request)));
    }

public:
    TPutRequest Request;
    TPutResponse Response;
};

class TKvRangeActor : public TKvBaseActor<TKvRangeActor> {
public:
    TKvRangeActor(ui64 logComponent, TString&& sessionId, TString&& path, TRangeRequest&& request)
        : TKvBaseActor<TKvRangeActor>(logComponent, std::move(sessionId), std::move(path))
        , Request(request) {
    }

    void Bootstrap() {
        RegisterKvRequest();
    }

    void RegisterKvRequest() {
        Become(&TKvRangeActor::KvStateFunc);

        Register(CreateKvRangeActor(LogComponent, SessionId, Path, TxControl, std::move(Request)));
    }

public:
    TRangeRequest Request;
    TRangeResponse Response;
};

class TKvTxnActor : public TKvBaseActor<TKvTxnActor> {
public:
    TKvTxnActor(ui64 logComponent, TString&& sessionId, TString&& path, TTxnRequest&& request)
        : TKvBaseActor<TKvTxnActor>(logComponent, std::move(sessionId), std::move(path))
        , Request(request) {
    }

    void Bootstrap() {
        RegisterRevisionIncRequest();
    }

    void RegisterKvRequest() {
        Become(&TKvTxnActor::KvStateFunc);

        Register(CreateKvTxnActor(LogComponent, SessionId, Path, TxControl, Revision, std::move(Request)));
    }

public:
    TTxnRequest Request;
    TTxnResponse Response;
};

} // anonymous namespace

NActors::IActor* CreateKvActor(ui64 logComponent, TString sessionId, TString path, TDeleteRequest request) {
    return new TKvDeleteActor(logComponent, std::move(sessionId), std::move(path), std::move(request));
}

NActors::IActor* CreateKvActor(ui64 logComponent, TString sessionId, TString path, TPutRequest request) {
    return new TKvPutActor(logComponent, std::move(sessionId), std::move(path), std::move(request));
}

NActors::IActor* CreateKvActor(ui64 logComponent, TString sessionId, TString path, TRangeRequest request) {
    return new TKvRangeActor(logComponent, std::move(sessionId), std::move(path), std::move(request));
}

NActors::IActor* CreateKvActor(ui64 logComponent, TString sessionId, TString path, TTxnRequest request) {
    return new TKvTxnActor(logComponent, std::move(sessionId), std::move(path), std::move(request));
}

} // namespace NYdb::NEtcd
