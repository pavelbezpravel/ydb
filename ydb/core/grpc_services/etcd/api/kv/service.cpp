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

etcdserverpb::RangeResponse FillResponse(const NYdb::NEtcd::TRangeResponse& response) {
    Y_UNUSED(response);
    return {};
}

etcdserverpb::PutResponse FillResponse(const NYdb::NEtcd::TPutResponse& response) {
    Y_UNUSED(response);
    return {};
}

etcdserverpb::DeleteRangeResponse FillResponse(const NYdb::NEtcd::TDeleteRangeResponse& response) {
    Y_UNUSED(response);
    return {};
}

etcdserverpb::TxnResponse FillResponse(const NYdb::NEtcd::TTxnResponse& response) {
    Y_UNUSED(response);
    return {};

}

etcdserverpb::CompactionResponse FillResponse(const NYdb::NEtcd::TCompactionResponse& response) {
    Y_UNUSED(response);
    return {};
}

template <typename TEvRequestType, typename TEvResponseType>
class TEtcdKVRequestRPC : public TActorBootstrapped<TEtcdKVRequestRPC<TEvRequestType, TEvResponseType>> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TEtcdKVRequestRPC(TEvRequestType* request)
        : Request_(request) {}

    void Bootstrap() {
        this->Become(&TEtcdKVRequestRPC::StateFunc);
        // TODO [pavelbezpravel]: WIP.

        this->Send(this->SelfId(), new TEvResponseType({}, {}, {}, {}));
        // auto req = *Request_->GetProtoRequest();
        // proto -> struct
        // this->Register(CreateKvActor(..., struct));
    }

private:
    STRICT_STFUNC(StateFunc, hFunc(TEvResponseType, Handle))
    void Handle(TEvResponseType::TPtr& ev) {
        // if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
        //     Request_->ReplyWithYdbStatus(Ydb::StatusIds::BAD_REQUEST);
        // }
        // else
        Request_->SendSerializedResult(std::move(FillResponse(ev->Get()->Response).SerializeAsString()), Ydb::StatusIds::SUCCESS);
        this->PassAway();
    }

private:
    std::unique_ptr<TEvRequestType> Request_;
};

} // namespace

namespace NEtcd {

using TEvRangeRequest = TGrpcRequestNoOperationCall<
    ::etcdserverpb::RangeRequest,
    ::etcdserverpb::RangeResponse>;

using TRangeRPC = TEtcdKVRequestRPC<
    TEvRangeRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvRangeResponse>;

void DoRange(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TEvRangeRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TRangeRPC(req));
}

using TEvPutRequest = TGrpcRequestNoOperationCall<
    ::etcdserverpb::PutRequest,
    ::etcdserverpb::PutResponse>;

using TPutRPC = TEtcdKVRequestRPC<
    TEvPutRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvPutResponse>;

void DoPut(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TEvPutRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TPutRPC(req));
}

using TEvDeleteRangeRequest = TGrpcRequestNoOperationCall<
    ::etcdserverpb::DeleteRangeRequest,
    ::etcdserverpb::DeleteRangeResponse>;

using TDeleteRangeRPC = TEtcdKVRequestRPC<
    TEvDeleteRangeRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvDeleteRangeResponse>;

void DoDeleteRange(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TEvDeleteRangeRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TDeleteRangeRPC(req));
}

using TEvTxnRequest = TGrpcRequestNoOperationCall<
    ::etcdserverpb::TxnRequest,
    ::etcdserverpb::TxnResponse>;

using TTxnRPC = TEtcdKVRequestRPC<
    TEvTxnRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvTxnResponse>;

void DoTxn(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TEvTxnRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TTxnRPC(req));
}

using TEvCompactionRequest = TGrpcRequestNoOperationCall<
    ::etcdserverpb::CompactionRequest,
    ::etcdserverpb::CompactionResponse>;

using TCompactRPC = TEtcdKVRequestRPC<
    TEvCompactionRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvCompactionResponse>;

void DoCompact(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TEvCompactionRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TCompactRPC(req));
}

} // namespace NEtcd

} // namespace NKikimr::NGRpcService
