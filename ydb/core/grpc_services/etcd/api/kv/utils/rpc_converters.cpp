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

    auto* kvs = out.mutable_kvs();
    kvs->Reserve(response.KVs.size());

    for (const auto& KV : response.KVs) {
        auto* kv = kvs->Add();
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

    auto* prevKVs = out.mutable_prev_kvs();
    prevKVs->Reserve(response.PrevKVs.size());

    for (const auto& PrevKV : response.PrevKVs) {
        auto* kv = prevKVs->Add();
        kv->set_key(PrevKV.key);
        kv->set_create_revision(PrevKV.create_revision);
        kv->set_mod_revision(PrevKV.mod_revision);
        kv->set_version(PrevKV.version);
        kv->set_value(PrevKV.value);
    }

    return out;
}

NYdb::NEtcd::TTxnRequest FillRequest(const etcdserverpb::TxnRequest& request) {
    auto out = NYdb::NEtcd::TTxnRequest{};

    out.Compare.reserve(request.compare_size());

    for (const auto& compare : request.compare()) {
        auto& Compare = out.Compare.emplace_back();
        Compare.Result = static_cast<NYdb::NEtcd::TTxnCompareRequest::ECompareResult>(compare.result());

        if (compare.has_create_revision()) {
            Compare.Target_create_revision = compare.create_revision();
        }
        if (compare.has_mod_revision()) {
            Compare.Target_mod_revision = compare.mod_revision();
        }
        if (compare.has_version()) {
            Compare.Target_version = compare.version();
        }
        if (compare.has_value()) {
            Compare.Target_value = compare.value();
        }

        Compare.Key = compare.key();
        Compare.Range_end = compare.range_end();
    }

    enum {SUCCESS, FAILURE};

    out.Requests.at(SUCCESS).reserve(request.success_size());
    for (const auto& req : request.success()) {
        if (req.has_request_range()) {
            out.Requests.at(SUCCESS).emplace_back(std::make_shared<NYdb::NEtcd::TRangeRequest>(FillRequest(req.request_range())));
        }
        if (req.has_request_put()) {
            out.Requests.at(SUCCESS).emplace_back(std::make_shared<NYdb::NEtcd::TPutRequest>(FillRequest(req.request_put())));
        }
        if (req.has_request_delete_range()) {
            out.Requests.at(SUCCESS).emplace_back(std::make_shared<NYdb::NEtcd::TDeleteRangeRequest>(FillRequest(req.request_delete_range())));
        }
        if (req.has_request_txn()) {
            out.Requests.at(SUCCESS).emplace_back(std::make_shared<NYdb::NEtcd::TTxnRequest>(FillRequest(req.request_txn())));
        }
    }

    out.Requests.at(FAILURE).reserve(request.failure_size());
    for (const auto& req : request.failure()) {
        if (req.has_request_range()) {
            out.Requests.at(FAILURE).emplace_back(std::make_shared<NYdb::NEtcd::TRangeRequest>(FillRequest(req.request_range())));
        }
        if (req.has_request_put()) {
            out.Requests.at(FAILURE).emplace_back(std::make_shared<NYdb::NEtcd::TPutRequest>(FillRequest(req.request_put())));
        }
        if (req.has_request_delete_range()) {
            out.Requests.at(FAILURE).emplace_back(std::make_shared<NYdb::NEtcd::TDeleteRangeRequest>(FillRequest(req.request_delete_range())));
        }
        if (req.has_request_txn()) {
            out.Requests.at(FAILURE).emplace_back(std::make_shared<NYdb::NEtcd::TTxnRequest>(FillRequest(req.request_txn())));
        }
    }

    return out;
}

etcdserverpb::TxnResponse FillResponse(const NYdb::NEtcd::TTxnResponse& response) {
    auto out = etcdserverpb::TxnResponse{};

    out.set_succeeded(response.Succeeded);

    auto* responses = out.mutable_responses();
    responses->Reserve(response.Responses.size());
    
    for (const auto& Response : response.Responses) {
        auto* response_op = responses->Add();
        std::visit([&response_op](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::shared_ptr<NYdb::NEtcd::TRangeResponse>>) {
                *response_op->mutable_response_range() = FillResponse(*arg.get());
            } else if constexpr (std::is_same_v<T, std::shared_ptr<NYdb::NEtcd::TPutResponse>>) {
                *response_op->mutable_response_put() = FillResponse(*arg.get());
            } else if constexpr (std::is_same_v<T, std::shared_ptr<NYdb::NEtcd::TDeleteRangeResponse>>) {
                *response_op->mutable_response_delete_range() = FillResponse(*arg.get());
            } else if constexpr (std::is_same_v<T, std::shared_ptr<NYdb::NEtcd::TTxnResponse>>) {
                *response_op->mutable_response_txn() = FillResponse(*arg.get());
            } else {
                static_assert(sizeof(T) == 0);
            }
        }, Response);
    }

    return out;
}

NYdb::NEtcd::TCompactionRequest FillRequest(const etcdserverpb::CompactionRequest& request) {
    return {
        request.revision(),
        request.physical()
    };
}

etcdserverpb::CompactionResponse FillResponse(const NYdb::NEtcd::TCompactionResponse& response) {
    Y_UNUSED(response);
    return {};
}

} // namespace NKikimr::NGRpcService::NEtcd