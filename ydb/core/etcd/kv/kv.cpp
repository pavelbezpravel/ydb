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
#include <ydb/core/etcd/revision/revision_inc.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NYdb::NEtcd {

namespace {

template<typename TEvReq, typename TEvResp>
class TKVActor : public NActors::TActorBootstrapped<TKVActor<TEvReq, TEvResp>> {
    using TReq = decltype(std::declval<TEvReq>().Request);
    using TResp = decltype(std::declval<TEvResp>().Response);

public:
    TKVActor(ui64 logComponent, TString&& sessionId, TString&& path, ui64 cookie, TReq&& request)
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

        RegisterRevisionIncRequest(currTxControl);
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TKVActor<TEvReq, TEvResp>>::Registered(sys, owner);
        Owner = owner;
    }

protected:
    void RegisterRevisionGetRequest(NKikimr::TQueryBase::TTxControl txControl) {
        this->Become(&TKVActor<TEvReq, TEvResp>::RevisionStateFunc);

        this->Register(CreateRevisionGetActor(LogComponent, SessionId, Path, txControl, TxId, Cookie));
    }

    void RegisterRevisionIncRequest(NKikimr::TQueryBase::TTxControl txControl) {
        this->Become(&TKVActor<TEvReq, TEvResp>::RevisionStateFunc);

        this->Register(CreateRevisionIncActor(LogComponent, SessionId, Path, txControl, TxId, Cookie));
    }

    STRICT_STFUNC(RevisionStateFunc, hFunc(TEvEtcdRevision::TEvRevisionResponse, Handle))

    void Handle(TEvEtcdRevision::TEvRevisionResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        SessionId = std::move(ev->Get()->SessionId);
        TxId = std::move(ev->Get()->TxId);
        Revision = ev->Get()->Revision;

        RegisterKVRequest(TxControl);
    }

    void RegisterKVRequest(NKikimr::TQueryBase::TTxControl txControl) {
        this->Become(&TKVActor<TEvReq, TEvResp>::KVStateFunc);

        this->Register(CreateKVQueryActor(LogComponent, SessionId, Path, txControl, TxId, Cookie, Revision, std::move(Request)));
    }

    STRICT_STFUNC(KVStateFunc, hFunc(TEvResp, Handle))

    void Handle(TEvResp::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            this->Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        SessionId = std::move(ev->Get()->SessionId);
        TxId = std::move(ev->Get()->TxId);
        Response = std::move(ev->Get()->Response);

        // if (!Response.IsWrite()) {
            this->Finish();
        // }

        // RegisterRevisionIncRequest(TxControl);
    }

    void Finish() {
        this->Finish(Ydb::StatusIds::SUCCESS, NYql::TIssues());
    }

    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        this->Send(Owner, new TEvResp(status, std::move(issues), SessionId, TxId, std::move(Response)), {}, Cookie);
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
    ui64 Cookie;
    TReq Request;
    TResp Response;
};

} // anonymous namespace

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TCompactionRequest request) {
    return new TKVActor<TEvEtcdKV::TEvCompactionRequest, TEvEtcdKV::TEvCompactionResponse>(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TDeleteRangeRequest request) {
    return new TKVActor<TEvEtcdKV::TEvDeleteRangeRequest, TEvEtcdKV::TEvDeleteRangeResponse>(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TPutRequest request) {
    return new TKVActor<TEvEtcdKV::TEvPutRequest, TEvEtcdKV::TEvPutResponse>(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TRangeRequest request) {
    return new TKVActor<TEvEtcdKV::TEvRangeRequest, TEvEtcdKV::TEvRangeResponse>(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TTxnRequest request) {
    return new TKVActor<TEvEtcdKV::TEvTxnRequest, TEvEtcdKV::TEvTxnResponse>(logComponent, std::move(sessionId), std::move(path), cookie, std::move(request));
}

} // namespace NYdb::NEtcd
