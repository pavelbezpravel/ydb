#include <ydb/core/etcd/kv/events.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <grpcpp/support/status.h>

namespace NKikimr::NGRpcService::NEtcd {

std::pair<grpc::StatusCode, TString> CheckResponse(Ydb::StatusIds::StatusCode status, const TString& message);

} // namespace NKikimr::NGRpcService::NEtcd
