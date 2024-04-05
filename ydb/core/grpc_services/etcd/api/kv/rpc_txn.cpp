#include "service.h"

#include <ydb/core/etcd/kv/events.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_request_base.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/public/api/etcd/api/etcdserverpb/rpc.grpc.pb.h>

namespace NKikimr::NGRpcService {

namespace {

using namespace NActors;

using TEvTxnRequest = TGrpcRequestNoOperationCall<::etcdserverpb::TxnRequest, ::etcdserverpb::TxnResponse>;

class TTxnRequestRPC : public TActorBootstrapped<TTxnRequestRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TTxnRequestRPC(TEvTxnRequest* request)
        : Request_(request)
    {}

    void Bootstrap() {
        Become(&TTxnRequestRPC::StateFunc);
        const auto request = *Request_->GetProtoRequest();
        Reply(request);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NYdb::NEtcd::TEvEtcdKV::TEvTxnResponse, Handle);
    )

    void Handle(NYdb::NEtcd::TEvEtcdKV::TEvTxnResponse::TPtr& ev) {
        Y_UNUSED(ev); // TODO [pavelbezpravel]: WIP.
        const auto request = *Request_->GetProtoRequest();
        Reply(request);
    }

    void Reply(const ::etcdserverpb::TxnRequest& request) {
        auto response = etcdserverpb::TxnResponse{};

        // TODO [pavelbezpravel]: WIP.
        Y_UNUSED(request);

        Request_->SendSerializedResult(std::move(response.SerializeAsString()), Ydb::StatusIds::SUCCESS);

        // TODO [pavelbezpravel]: introduce Finish() method.
        PassAway();
    }

private:
    std::unique_ptr<TEvTxnRequest> Request_;
};

} // namespace

namespace NEtcd {

void DoTxn(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TEvTxnRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TTxnRequestRPC(req));
}

}

} // namespace NKikimr::NGRpcService
