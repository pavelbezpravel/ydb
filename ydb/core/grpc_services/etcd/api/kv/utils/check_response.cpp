#include "check_response.h"

namespace NKikimr::NGRpcService::NEtcd {

std::pair<grpc::StatusCode, TString> CheckResponse(Ydb::StatusIds::StatusCode status, const TString& message) {
    if (status == Ydb::StatusIds::PRECONDITION_FAILED && message.compare("etcdserver: key not found") == 0) {
        return {grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: key not found"};
    }

    // TODO [pavelbezpravel]: ErrGRPCFutureRev
    if (message.compare("etcdserver: mvcc: required revision is a future revision") == 0) {
        return {grpc::StatusCode::OUT_OF_RANGE, "etcdserver: mvcc: required revision is a future revision"};
    }

    // TODO [pavelbezpravel]: ErrGRPCCompacted
    if (message.compare("etcdserver: mvcc: required revision has been compacted") == 0) {
        return {grpc::StatusCode::OUT_OF_RANGE, "etcdserver: mvcc: required revision has been compacted"};
    }

    return {grpc::StatusCode::UNIMPLEMENTED, message};
}

} // namespace NKikimr::NGRpcService::NEtcd
