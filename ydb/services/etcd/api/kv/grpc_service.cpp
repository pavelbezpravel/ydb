#include "grpc_service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/counters/counters.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/etcd/api/kv/service.h>


namespace NKikimr::NGRpcService {

TGRpcEtcdKVService::TGRpcEtcdKVService(
    NActors::TActorSystem *system,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const NActors::TActorId &proxyId, bool rlAllowed,
    size_t handlersPerCompletionQueue)
    : TGrpcServiceBase(system, counters, proxyId, rlAllowed),
      HandlersPerCompletionQueue(Max(size_t{1}, handlersPerCompletionQueue)) {}

TGRpcEtcdKVService::TGRpcEtcdKVService(
    NActors::TActorSystem *system,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const TVector<NActors::TActorId> &proxies, bool rlAllowed,
    size_t handlersPerCompletionQueue)
    : TGrpcServiceBase(system, counters, proxies, rlAllowed),
      HandlersPerCompletionQueue(Max(size_t{1}, handlersPerCompletionQueue)) {}

void TGRpcEtcdKVService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace etcdserverpb;
    using namespace NEtcd;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    size_t proxyCounter = 0;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, CB, ...) \
    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) { \
        for (auto* cq: CQS) { \
            MakeIntrusive<TGRpcRequest<IN, OUT, TGRpcEtcdKVService>>(this, &Service_, cq, \
                [this, proxyCounter](NYdbGrpc::IRequestContextBase* ctx) { \
                    NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
                    ActorSystem_->Send(GRpcProxies_[proxyCounter % GRpcProxies_.size()], \
                        new TGrpcRequestNoOperationCall<IN, OUT> \
                            (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr __VA_OPT__(, TAuditMode::__VA_ARGS__)})); \
                }, &etcdserverpb::KV::AsyncService::Request ## NAME, \
                #NAME, logger, getCounterBlock("KV", #NAME))->Run(); \
            ++proxyCounter; \
        } \
    }

    // TODO [pavelbezpravel]: WIP.

    ADD_REQUEST(Range, RangeRequest, RangeResponse, DoRange);
    // ADD_REQUEST(Put, PutRequest, PutResponse, DoPut);
    // ADD_REQUEST(DeleteRange, DeleteRangeRequest, DeleteRangeResponse, DoDeleteRange);
    // ADD_REQUEST(Txn, TxnRequest, TxnResponse, DoTxn);
    // ADD_REQUEST(Compact, CompactionRequest, CompactionResponse, DoCompact);

#undef ADD_REQUEST
}

} // namespace NKikimr::NGRpcService
