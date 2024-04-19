#include "kv.h"

#include "events.h"
#include "proto.h"

#include "kv_delete.h"
#include "kv_put.h"
#include "kv_range.h"
#include "kv_txn.h"

#include <utility>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/core/etcd/revision/events.h>
#include <ydb/core/etcd/revision/revision_inc.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NYdb::NEtcd {

namespace {

class TKVDeleteRangeActor;
class TKVPutActor;
class TKVRangeActor;
class TKVTxnActor;

template<typename TActor>
struct TResponseTraits;

template<>
struct TResponseTraits<TKVDeleteRangeActor> {
    using type = TEvEtcdKV::TEvDeleteRangeResponse;
};

template<>
struct TResponseTraits<TKVPutActor> {
    using type = TEvEtcdKV::TEvPutResponse;
};

template<>
struct TResponseTraits<TKVRangeActor> {
    using type = TEvEtcdKV::TEvRangeResponse;
};

template<>
struct TResponseTraits<TKVTxnActor> {
    using type = TEvEtcdKV::TEvTxnResponse;
};

template<typename TDerived>
class TKVBaseActor : public NActors::TActorBootstrapped<TDerived> {
public:
    TKVBaseActor(ui64 logComponent, TString&& sessionId, TString&& path, uint64_t cookie)
        : LogComponent(logComponent)
        , SessionId(sessionId)
        , Path(path)
        , TxControl(NKikimr::TQueryBase::TTxControl::BeginAndCommitTx())
        , Cookie(cookie) {
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TKVBaseActor<TDerived>>::Registered(sys, owner);
        Owner = owner;
    }

protected:
    using TEvKVResponse = typename TResponseTraits<TDerived>::type;

    TDerived& GetDerived() {
        return static_cast<TDerived&>(*this);
    }

    void RegisterRevisionIncRequest() {
        auto [currTxControl, nextTxControl] = TQueryBase::Split(TxControl);
        TxControl = nextTxControl;

        this->Become(&TKVBaseActor<TDerived>::RevisionStateFunc);

        this->Register(CreateRevisionIncActor(LogComponent, SessionId, Path, currTxControl, TxId, Cookie));
    }

    STRICT_STFUNC(RevisionStateFunc, hFunc(TEvEtcdRevision::TEvRevisionResponse, Handle))
    void Handle(TEvEtcdRevision::TEvRevisionResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        TxId = std::move(ev->Get()->TxId);
        Revision = ev->Get()->Revision;

        GetDerived().RegisterKVRequest();
    }

    STRICT_STFUNC(KVStateFunc, hFunc(TEvKVResponse, Handle))

    void Handle(TEvKVResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        GetDerived().Response = std::move(ev->Get()->Response);

        Finish();
    }

    void Finish() {
        Finish(Ydb::StatusIds::SUCCESS, NYql::TIssues());
    }

    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        this->Send(Owner, new TEvKVResponse(status, std::move(issues), TxId, std::move(GetDerived().Response)), {}, Cookie);
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
    uint64_t Cookie;
};

class TKVDeleteRangeActor : public TKVBaseActor<TKVDeleteRangeActor> {
public:
    TKVDeleteRangeActor(ui64 logComponent, TString&& sessionId, TString&& path, uint64_t cookie, TDeleteRangeRequest&& request)
        : TKVBaseActor<TKVDeleteRangeActor>(logComponent, std::move(sessionId), std::move(path), cookie)
        , Request(request) {
    }

    void Bootstrap() {
        RegisterRevisionIncRequest();
    }

    void RegisterKVRequest() {
        Become(&TKVDeleteRangeActor::KVStateFunc);

        Register(CreateKVDeleteActor(LogComponent, SessionId, Path, TxControl, TxId, Revision, Cookie, std::move(Request)));
    }

public:
    TDeleteRangeRequest Request;
    TDeleteRangeResponse Response;
};

class TKVPutActor : public TKVBaseActor<TKVPutActor> {
public:
    TKVPutActor(ui64 logComponent, TString&& sessionId, TString&& path, uint64_t cookie, TPutRequest&& request)
        : TKVBaseActor<TKVPutActor>(logComponent, std::move(sessionId), std::move(path), cookie)
        , Request(request) {
    }

    void Bootstrap() {
        RegisterRevisionIncRequest();
    }

    void RegisterKVRequest() {
        Become(&TKVPutActor::KVStateFunc);

        Register(CreateKVPutActor(LogComponent, SessionId, Path, TxControl, TxId, Revision, Cookie, std::move(Request)));
    }

public:
    TPutRequest Request;
    TPutResponse Response;
};

class TKVRangeActor : public TKVBaseActor<TKVRangeActor> {
public:
    TKVRangeActor(ui64 logComponent, TString&& sessionId, TString&& path, uint64_t cookie, TRangeRequest&& request)
        : TKVBaseActor<TKVRangeActor>(logComponent, std::move(sessionId), std::move(path), cookie)
        , Request(request) {
    }

    void Bootstrap() {
        RegisterKVRequest();
    }

    void RegisterKVRequest() {
        Become(&TKVRangeActor::KVStateFunc);

        Register(CreateKVRangeActor(LogComponent, SessionId, Path, TxControl, TxId, Cookie, std::move(Request)));
    }

public:
    TRangeRequest Request;
    TRangeResponse Response;
};

class TKVTxnActor : public TKVBaseActor<TKVTxnActor> {
public:
    TKVTxnActor(ui64 logComponent, TString&& sessionId, TString&& path, uint64_t cookie, TTxnRequest&& request)
        : TKVBaseActor<TKVTxnActor>(logComponent, std::move(sessionId), std::move(path), cookie)
        , Request(request) {
    }

    void Bootstrap() {
        RegisterRevisionIncRequest();
    }

    void RegisterKVRequest() {
        Become(&TKVTxnActor::KVStateFunc);

        Register(CreateKVTxnActor(LogComponent, SessionId, Path, TxControl, TxId, Revision, Cookie, std::move(Request)));
    }

public:
    TTxnRequest Request;
    TTxnResponse Response;
};

} // anonymous namespace

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, uint64_t cookie, TDeleteRangeRequest request) {
    return new TKVDeleteRangeActor(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, uint64_t cookie, TPutRequest request) {
    return new TKVPutActor(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, uint64_t cookie, TRangeRequest request) {
    return new TKVRangeActor(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, uint64_t cookie, TTxnRequest request) {
    return new TKVTxnActor(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

} // namespace NYdb::NEtcd
