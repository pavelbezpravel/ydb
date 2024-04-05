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

using TEvRangeRequest = TGrpcRequestNoOperationCall<::etcdserverpb::RangeRequest, ::etcdserverpb::RangeResponse>;

class TRangeRequestRPC : public TActorBootstrapped<TRangeRequestRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TRangeRequestRPC(TEvRangeRequest* request)
        : Request_(request)
    {}

    void Bootstrap() {
        Become(&TRangeRequestRPC::StateFunc);
        const auto request = *Request_->GetProtoRequest();
        Reply(request);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NYdb::NEtcd::TEvEtcdKV::TEvRangeResponse, Handle);
    )

    void Handle(NYdb::NEtcd::TEvEtcdKV::TEvRangeResponse::TPtr& ev) {
        Y_UNUSED(ev); // TODO [pavelbezpravel]: WIP.
        const auto request = *Request_->GetProtoRequest();
        Reply(request);
    }

    void Reply(const ::etcdserverpb::RangeRequest& request) {
        auto response = etcdserverpb::RangeResponse{};

        // TODO [pavelbezpravel]: WIP.
        Y_UNUSED(request);

        Request_->SendSerializedResult(std::move(response.SerializeAsString()), Ydb::StatusIds::SUCCESS);

        // TODO [pavelbezpravel]: introduce Finish() method.
        PassAway();
    }

private:
    std::unique_ptr<TEvRangeRequest> Request_;
};

} // namespace

namespace NEtcd {

void DoRange(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TEvRangeRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TRangeRequestRPC(req));
}

}

} // namespace NKikimr::NGRpcService