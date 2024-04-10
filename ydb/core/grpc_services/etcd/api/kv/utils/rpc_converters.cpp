#include "rpc_converters.h"

namespace NKikimr::NGRpcService::NEtcd {

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

    if (response.PrevKVs.empty()) {
        return {};
    }

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
    return {};
}

etcdserverpb::TxnResponse FillResponse(const NYdb::NEtcd::TTxnResponse& response) {
    auto out = etcdserverpb::TxnResponse{};
    Y_UNUSED(response);



    return out;

}

NYdb::NEtcd::TTxnCompareRequest FillRequest(const etcdserverpb::CompactionRequest& request) {
    Y_UNUSED(request);
    return {};
}

etcdserverpb::CompactionResponse FillResponse(const NYdb::NEtcd::TCompactionResponse& response) {
    Y_UNUSED(response);
    return {};
}

} // namespace NKikimr::NGRpcService::NEtcd