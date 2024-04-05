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

using TEvPutRequest = TGrpcRequestNoOperationCall<::etcdserverpb::PutRequest, ::etcdserverpb::PutResponse>;

class TPutRequestRPC : public TActorBootstrapped<TPutRequestRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TPutRequestRPC(TEvPutRequest* request)
        : Request_(request)
    {}

    void Bootstrap() {
        Become(&TPutRequestRPC::StateFunc);
        const auto request = *Request_->GetProtoRequest();
        Reply(request);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NYdb::NEtcd::TEvEtcdKV::TEvPutResponse, Handle);
    )

    void Handle(NYdb::NEtcd::TEvEtcdKV::TEvPutResponse::TPtr& ev) {
        Y_UNUSED(ev); // TODO [pavelbezpravel]: WIP.
        const auto request = *Request_->GetProtoRequest();
        Reply(request);
    }

    void Reply(const ::etcdserverpb::PutRequest& request) {
        auto response = etcdserverpb::PutResponse{};

        // TODO [pavelbezpravel]: WIP.
        Y_UNUSED(request);

        Request_->SendSerializedResult(std::move(response.SerializeAsString()), Ydb::StatusIds::SUCCESS);

        // TODO [pavelbezpravel]: introduce Finish() method.
        PassAway();
    }

private:
    std::unique_ptr<TEvPutRequest> Request_;
};

} // namespace

namespace NEtcd {

void DoPut(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TEvPutRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TPutRequestRPC(req));
}

}

} // namespace NKikimr::NGRpcService
