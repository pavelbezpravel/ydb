#include <ydb/public/api/etcd/api/etcdserverpb/rpc.grpc.pb.h>

#include <grpcpp/support/status.h>

namespace NKikimr::NGRpcService::NEtcd {

grpc::Status CheckRequest(const etcdserverpb::RangeRequest& request);
grpc::Status CheckRequest(const etcdserverpb::PutRequest& request);
grpc::Status CheckRequest(const etcdserverpb::DeleteRangeRequest& request);
grpc::Status CheckRequest(const etcdserverpb::TxnRequest& request);
grpc::Status CheckRequest(const etcdserverpb::CompactionRequest& request);

} // namespace NKikimr::NGRpcService::NEtcd
