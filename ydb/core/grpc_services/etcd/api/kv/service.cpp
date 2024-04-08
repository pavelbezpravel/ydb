#include "service.h"

#include <ydb/core/etcd/kv/events.h>
#include <ydb/core/etcd/kv/kv.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_request_base.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/public/api/etcd/api/etcdserverpb/rpc.grpc.pb.h>

namespace NKikimr::NGRpcService {

namespace {

using namespace NActors;

NYdb::NEtcd::TRangeRequest FillRequest(const etcdserverpb::RangeRequest& request) {
    return {
        request.key(),
        request.range_end(),
        static_cast<size_t>(request.limit()),
        request.revision(),
        static_cast<NYdb::NEtcd::TRangeRequest::ESortOrder>(request.sort_order()),
        static_cast<NYdb::NEtcd::TRangeRequest::ESortTarget>(request.sort_target()),
        request.serializable(),
        request.keys_only(),
        request.count_only(),
        request.min_mod_revision(),
        request.max_mod_revision(),
        request.min_create_revision(),
        request.max_create_revision()
    };
}

etcdserverpb::RangeResponse FillResponse(const NYdb::NEtcd::TRangeResponse& response) {
    auto out = etcdserverpb::RangeResponse{};

    for (const auto KVs = response.KVs; const auto& KV : KVs) {
        auto* kv = out.add_kvs();
        kv->set_key(KV.key);
        kv->set_create_revision(KV.create_revision);
        kv->set_mod_revision(KV.mod_revision);
        kv->set_version(KV.version);
        kv->set_value(KV.value);
    }
    out.set_more(response.More);
    out.set_count(response.Count);

    return out;
}

NYdb::NEtcd::TPutRequest FillRequest(const etcdserverpb::PutRequest& request) {
    return {
        {{request.key(), request.value()}},
        request.prev_kv(),
        request.ignore_value()
    };
}

etcdserverpb::PutResponse FillResponse(const NYdb::NEtcd::TPutResponse& response) {
    auto out = etcdserverpb::PutResponse{};
    auto PrevKV = response.PrevKVs.front();
    auto* kv = out.mutable_prev_kv();

    kv->set_key(PrevKV.key);
    kv->set_create_revision(PrevKV.create_revision);
    kv->set_mod_revision(PrevKV.mod_revision);
    kv->set_version(PrevKV.version);
    kv->set_value(PrevKV.value);

    return out;
}

NYdb::NEtcd::TDeleteRangeRequest FillRequest(const etcdserverpb::DeleteRangeRequest& request) {
    return {
        request.key(),
        request.range_end(),
        request.prev_kv()
    };
}

etcdserverpb::DeleteRangeResponse FillResponse(const NYdb::NEtcd::TDeleteRangeResponse& response) {
    auto out = etcdserverpb::DeleteRangeResponse{};

    out.set_deleted(response.Deleted);
    for (const auto PrevKVs = response.PrevKVs; const auto& PrevKV : PrevKVs){
        auto* kv= out.add_prev_kvs();
        kv->set_key(PrevKV.key);
        kv->set_create_revision(PrevKV.create_revision);
        kv->set_mod_revision(PrevKV.mod_revision);
        kv->set_version(PrevKV.version);
        kv->set_value(PrevKV.value);
    }

    return out;
}

NYdb::NEtcd::TTxnRequest FillRequest(const etcdserverpb::TxnRequest& request) {
    Y_UNUSED(request);
    return {

    };
}

etcdserverpb::TxnResponse FillResponse(const NYdb::NEtcd::TTxnResponse& response) {
    auto out = etcdserverpb::TxnResponse{};
    Y_UNUSED(response);



    return out;

}

// NYdb::NEtcd::TTxnCompareRequest FillRequest(const etcdserverpb::CompactionRequest& request) {
//     Y_UNUSED(request);
//     return {};
// }

// etcdserverpb::CompactionResponse FillResponse(const NYdb::NEtcd::TCompactionResponse& response) {
//     Y_UNUSED(response);
//     return {};
// }

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
        Y_UNUSED(FillRequest(*Request_->GetProtoRequest()));
        // this->Register(NYdb::NEtcd::CreateKVActor(NKikimrServices::KQP_PROXY, {}, {}, FillRequest(*Request_->GetProtoRequest())));
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

// TODO [pavelbezpravel]: WIP.
void DoCompact(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    // auto* req = dynamic_cast<TEvCompactionRequest*>(p.release());
    // Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    // f.RegisterActor(new TCompactRPC(req));
    Y_UNUSED(p);
    Y_UNUSED(f);
}

} // namespace NEtcd

} // namespace NKikimr::NGRpcService
