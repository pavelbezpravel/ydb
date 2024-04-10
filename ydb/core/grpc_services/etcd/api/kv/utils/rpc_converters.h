#pragma once

#include <ydb/core/etcd/kv/proto.h>

#include <ydb/public/api/etcd/api/etcdserverpb/rpc.grpc.pb.h>

namespace NKikimr::NGRpcService::NEtcd {

NYdb::NEtcd::TRangeRequest FillRequest(const etcdserverpb::RangeRequest& request);
etcdserverpb::RangeResponse FillResponse(const NYdb::NEtcd::TRangeResponse& response);

NYdb::NEtcd::TPutRequest FillRequest(const etcdserverpb::PutRequest& request);
etcdserverpb::PutResponse FillResponse(const NYdb::NEtcd::TPutResponse& response);

NYdb::NEtcd::TDeleteRangeRequest FillRequest(const etcdserverpb::DeleteRangeRequest& request);
etcdserverpb::DeleteRangeResponse FillResponse(const NYdb::NEtcd::TDeleteRangeResponse& response);

NYdb::NEtcd::TTxnRequest FillRequest(const etcdserverpb::TxnRequest& request);
etcdserverpb::TxnResponse FillResponse(const NYdb::NEtcd::TTxnResponse& response);

NYdb::NEtcd::TCompactionRequest FillRequest(const etcdserverpb::CompactionRequest& request);
etcdserverpb::CompactionResponse FillResponse(const NYdb::NEtcd::TCompactionResponse& response);

} // namespace NKikimr::NGRpcService::NEtcd
