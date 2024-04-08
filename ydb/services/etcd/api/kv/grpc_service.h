#pragma once

#include <ydb/core/grpc_services/base/base_service.h>

#include <ydb/public/api/etcd/api/etcdserverpb/rpc.grpc.pb.h>

namespace NKikimr::NGRpcService {

class TGRpcEtcdKVService : public TGrpcServiceBase<etcdserverpb::KV>
{
public:
    using TGrpcServiceBase<etcdserverpb::KV>::TGrpcServiceBase;

    TGRpcEtcdKVService(
        NActors::TActorSystem *system,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const NActors::TActorId& proxyId,
        bool rlAllowed,
        size_t handlersPerCompletionQueue = 1);

    TGRpcEtcdKVService(
        NActors::TActorSystem *system,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const TVector<NActors::TActorId>& proxies,
        bool rlAllowed,
        size_t handlersPerCompletionQueue);

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

private:
    const size_t HandlersPerCompletionQueue;
};

} // namespace NKikimr::NGRpcService
