#include "service.h"
#include "utils/rpc_converters.h"

#include <ydb/core/etcd/kv/events.h>
#include <ydb/core/etcd/service/service.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_request_base.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/public/api/etcd/api/etcdserverpb/rpc.grpc.pb.h>

namespace NKikimr::NGRpcService {

template <typename RpcRequestType, typename EvRequestType, typename EvResponseType>
class TEtcdKVRequestRPC : public TRpcRequestActor<TEtcdKVRequestRPC<RpcRequestType, EvRequestType, EvResponseType>, RpcRequestType, false> {
public:
    using TRpcRequestActorBase = TRpcRequestActor<TEtcdKVRequestRPC<RpcRequestType, EvRequestType, EvResponseType>, RpcRequestType, false>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TEtcdKVRequestRPC(RpcRequestType* request)
        : TRpcRequestActorBase(request) {}

    void Bootstrap() {
        this->Become(&TEtcdKVRequestRPC::StateFunc);

        const auto* req = this->GetProtoRequest();

        if (!req) {
            this->Request->ReplyWithRpcStatus(grpc::StatusCode::INTERNAL, "Internal error");
            this->PassAway();
            return;
        }

        this->Send(NYdb::NEtcd::MakeEtcdServiceId(), new EvRequestType(NEtcd::FillRequest(*req)));
    }

private:
    STRICT_STFUNC(StateFunc, hFunc(EvResponseType, Handle))
    void Handle(EvResponseType::TPtr& ev) {
        if (const auto response = ev->Get()->Response; ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            auto out = NEtcd::FillResponse(response);
            this->Request->Reply(&out, grpc::StatusCode::OK);
        }
        else {
            // TODO [pavelbezpravel]: fill status and error message.
            this->Request->ReplyWithRpcStatus(grpc::StatusCode::CANCELLED, {});
        }
        this->PassAway();
    }
};

namespace NEtcd {

using TRpcRangeRequest = TGrpcRequestNoOperationCall<
    ::etcdserverpb::RangeRequest,
    ::etcdserverpb::RangeResponse>;

using TRangeRPC = TEtcdKVRequestRPC<
    TRpcRangeRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvRangeRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvRangeResponse>;

void DoRange(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TRpcRangeRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TRangeRPC(req));
}

using TRpcPutRequest = TGrpcRequestNoOperationCall<
    ::etcdserverpb::PutRequest,
    ::etcdserverpb::PutResponse>;

using TPutRPC = TEtcdKVRequestRPC<
    TRpcPutRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvPutRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvPutResponse>;

void DoPut(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TRpcPutRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TPutRPC(req));
}

using TRpcDeleteRangeRequest = TGrpcRequestNoOperationCall<
    ::etcdserverpb::DeleteRangeRequest,
    ::etcdserverpb::DeleteRangeResponse>;

using TDeleteRangeRPC = TEtcdKVRequestRPC<
    TRpcDeleteRangeRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvDeleteRangeRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvDeleteRangeResponse>;

void DoDeleteRange(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TRpcDeleteRangeRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TDeleteRangeRPC(req));
}

using TRpcTxnRequest = TGrpcRequestNoOperationCall<
    ::etcdserverpb::TxnRequest,
    ::etcdserverpb::TxnResponse>;

using TTxnRPC = TEtcdKVRequestRPC<
    TRpcTxnRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvTxnRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvTxnResponse>;

void DoTxn(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TRpcTxnRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TTxnRPC(req));
}

using TRpcCompactionRequest = TGrpcRequestNoOperationCall<
    ::etcdserverpb::CompactionRequest,
    ::etcdserverpb::CompactionResponse>;

using TCompactRPC = TEtcdKVRequestRPC<
    TRpcCompactionRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvCompactionRequest,
    NYdb::NEtcd::TEvEtcdKV::TEvCompactionResponse>;

void DoCompact(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* req = dynamic_cast<TRpcCompactionRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TCompactRPC(req));
}

} // namespace NEtcd

} // namespace NKikimr::NGRpcService
