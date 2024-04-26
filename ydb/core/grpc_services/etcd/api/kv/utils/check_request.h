#include <ydb/public/api/etcd/api/etcdserverpb/rpc.grpc.pb.h>

#include <grpcpp/support/status.h>

namespace NKikimr::NGRpcService::NEtcd {

// kMaxTxnOps is the max operations per txn.
// e.g suppose kMmaxTxnOps = 128.
// Txn.Success can have at most 128 operations,
// and Txn.Failure can have at most 128 operations.
static constexpr auto kMaxTxnOps = 128;

grpc::Status CheckRequest(const etcdserverpb::RangeRequest& request);
grpc::Status CheckRequest(const etcdserverpb::PutRequest& request);
grpc::Status CheckRequest(const etcdserverpb::DeleteRangeRequest& request);
grpc::Status CheckRequest(const etcdserverpb::TxnRequest& request, int maxTxnOps = kMaxTxnOps);
grpc::Status CheckRequest(const etcdserverpb::CompactionRequest& request);

} // namespace NKikimr::NGRpcService::NEtcd
