#include "check_request.h"

namespace NKikimr::NGRpcService::NEtcd {

grpc::Status CheckRequest(const etcdserverpb::RangeRequest& request) {
    if (request.key().empty()) {
        return {grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: key is not provided"};
    }

    if (const auto sort_order = etcdserverpb::RangeRequest::SortOrder_Name(request.sort_order());
        sort_order.empty())
    {
        return {grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: invalid sort option"};
    }

    if (const auto sort_target = etcdserverpb::RangeRequest::SortTarget_Name(request.sort_target());
        sort_target.empty())
    {
        return {grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: invalid sort option"};
    }

    return {};
}

grpc::Status CheckRequest(const etcdserverpb::PutRequest& request) {
    if (request.key().empty()) {
        return {grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: key is not provided"};
    }

    if (request.ignore_value() && !request.value().empty()) {
        return {grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: value is provided"};
    }

    if (request.ignore_lease() && request.lease() != 0) {
        return {grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: lease is provided"};
    }

    return {};
}

grpc::Status CheckRequest(const etcdserverpb::DeleteRangeRequest& request) {
    if (request.key().empty()) {
        return {grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: key is not provided"};
    }

    return {};
}

grpc::Status CheckRequestOp(const etcdserverpb::RequestOp& operation, int maxTxnOps);

grpc::Status CheckRequest(const etcdserverpb::TxnRequest& request, int maxTxnOps) {
    auto opc = request.compare_size();
    if (opc < request.success_size()) {
        opc = request.success_size();
    }
    if (opc < request.failure_size()) {
        opc = request.failure_size();
    }
    if (opc > maxTxnOps) {
        return {grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: too many operations in txn request"};
    }

    if (const auto compare = request.compare();
        std::any_of(compare.begin(),compare.end(), [](const auto& el) { return el.key().empty(); }))
    {
        return {grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: key is not provided"};
    }

    for (const auto& req : request.success()) {
        const auto status = CheckRequestOp(req, maxTxnOps - opc);
        if (!status.ok()) {
            return status;
        }
    }

    for (const auto& req : request.failure()) {
        const auto status = CheckRequestOp(req, maxTxnOps - opc);
        if (!status.ok()) {
            return status;
        }
    }

    // TODO [pavelbezpravel]: checkIntervals

    return {};
}

grpc::Status CheckRequest(const etcdserverpb::CompactionRequest&) {
    // There are no checks for CompactionRequest in etcd.
    return {};
}

grpc::Status CheckRequestOp(const etcdserverpb::RequestOp& operation, int maxTxnOps) {
    if (operation.has_request_range())  {
        return CheckRequest(operation.request_range());
    }
    else if (operation.has_request_put()) {
        return CheckRequest(operation.request_put());
    }
    else if (operation.has_request_delete_range()) {
        return CheckRequest(operation.request_delete_range());
    }
    else if (operation.has_request_txn()) {
        return CheckRequest(operation.request_txn(), maxTxnOps);
    }
    else {
        return {grpc::StatusCode::INVALID_ARGUMENT, "etcdserver: key not found"};
    }
}

} // namespace NKikimr::NGRpcService::NEtcd
