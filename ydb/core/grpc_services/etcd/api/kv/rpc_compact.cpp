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

using TEvCompactionRequest = TGrpcRequestNoOperationCall<::etcdserverpb::CompactionRequest, ::etcdserverpb::CompactionResponse>;

class TCompactionRequestRPC : public TActorBootstrapped<TCompactionRequestRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TCompactionRequestRPC(TEvCompactionRequest* request)
        : Request_(request)
    {}

    void Bootstrap() {
        Become(&TCompactionRequestRPC::StateFunc);
        const auto request = *Request_->GetProtoRequest();
        Reply(request);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NYdb::NEtcd::TEvEtcdKV::TEvCompactionResponse, Handle);
    )

    void Handle(NYdb::NEtcd::TEvEtcdKV::TEvCompactionResponse::TPtr& ev) {
        Y_UNUSED(ev); // TODO [pavelbezpravel]: WIP.
        const auto request = *Request_->GetProtoRequest();
        Reply(request);
    }

    void Reply(const ::etcdserverpb::CompactionRequest& request) {
        auto response = etcdserverpb::CompactionResponse{};

        // TODO [pavelbezpravel]: WIP.
        Y_UNUSED(request);

        Request_->SendSerializedResult(std::move(response.SerializeAsString()), Ydb::StatusIds::SUCCESS);

        // TODO [pavelbezpravel]: introduce Finish() method.
        PassAway();
    }

private:
    std::unique_ptr<TEvCompactionRequest> Request_;
};

} // namespace

namespace NEtcd {

void DoCompact(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TEvCompactionRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TCompactionRequestRPC(req));
}

}

} // namespace NKikimr::NGRpcService
