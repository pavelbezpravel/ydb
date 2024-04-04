#include "service_kv.h"

#include <ydb/core/etcd/events.h>
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
        const auto& request = *Request_->GetProtoRequest();
        Reply(request);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NKikimr::NEtcd::TEvEtcd::TEvRangeResponse, Handle);
    )

    void Handle(NKikimr::NEtcd::TEvEtcd::TEvRangeResponse::TPtr& ev) {
        Y_UNUSED(ev); // TODO [pavelbezpravel]: WIP.
        const auto& request = *Request_->GetProtoRequest();
        Reply(request);
    }

    void Reply(const ::etcdserverpb::RangeRequest& request) {
        etcdserverpb::RangeResponse response{};
        auto* kv = response.add_kvs();
        kv->set_key(request.key());
        kv->set_value("TODO [pavelbezpravel]: Range stub.");
        Request_->SendSerializedResult(std::move(response.SerializeAsString()), Ydb::StatusIds::SUCCESS);
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