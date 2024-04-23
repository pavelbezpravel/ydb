#pragma once

#include <array>
#include <memory>
#include <variant>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NYdb::NEtcd {

struct TKeyValue {
    TString key;
    i64 mod_revision;
    i64 create_revision;
    i64 version;
    TString value;

    friend IOutputStream& operator<<(IOutputStream& str, const TKeyValue& data) {
        str << "{ "
            << "key: \"" << data.key << "\", "
            << "value: \"" << data.value << "\", "
            << "create_revision: " << data.create_revision << ", "
            << "mod_revision: " << data.mod_revision << ", "
            << "version: " << data.version
            << " }";
        return str;
    }
};

struct TRangeRequest {
    enum class ESortOrder {
        NONE,
        ASCEND,
        DESCEND,
    };
    enum class ESortTarget {
        KEY,
        CREATE,
        MOD,
        VERSION,
        VALUE,
    };

    TString key;
    TString range_end;
    size_t limit;
    i64 revision;
    ESortOrder sort_order;
    ESortTarget sort_target;
    bool serializable;
    bool keys_only;
    bool count_only;
    i64 min_mod_revision;
    i64 max_mod_revision;
    i64 min_create_revision;
    i64 max_create_revision;

    friend IOutputStream& operator<<(IOutputStream& str, const TRangeRequest& data) {
        str << "[TRangeRequest] { "
            << "key: \"" << data.key << "\", "
            << "range_end: \"" << data.range_end << "\", "
            << "limit: " << data.limit << ", "
            << "revision: " << data.revision << ", "
            << "sort_order: " << std::underlying_type_t<ESortOrder>(data.sort_order) << ", "
            << "sort_target: " << std::underlying_type_t<ESortTarget>(data.sort_target) << ", "
            << "serializable: " << data.serializable << ", "
            << "keys_only: " << data.keys_only << ", "
            << "count_only: " << data.count_only << ", "
            << "min_mod_revision: " << data.min_mod_revision << ", "
            << "max_mod_revision: " << data.max_mod_revision << ", "
            << "min_create_revision: " << data.min_create_revision << ", "
            << "max_create_revison: " << data.max_create_revision
            << " }";
        return str;
    }
};

struct TRangeResponse {
    TVector<TKeyValue> KVs;
    bool More;
    size_t Count;

    friend IOutputStream& operator<<(IOutputStream& str, const TRangeResponse& data) {
        str << "[TRangeResponse] { "
            << "More: " << data.More << ", "
            << "Count: " << data.Count << ", "
            << "KVs: ";

        for (const auto& kv : data.KVs) {
            str << kv;
        }

        str << " }";
        return str;
    }

    [[nodiscard]] constexpr bool IsWrite() const noexcept {
        return false;
    }
};

struct TPutRequest {
    TVector<std::pair<TString, TString>> KVs;
    bool PrevKV;
    bool IgnoreValue;

    friend IOutputStream& operator<<(IOutputStream& str, const TPutRequest& data) {
        str << "[TPutRequest] { "
            << "PrevKV: " << data.PrevKV << ", "
            << "IgnoreValue: " << data.IgnoreValue << ", "
            << "KVs: { ";
        
        for (const auto& [key, value] : data.KVs) {
            str << "key: \"" << key << "\", value: \"" << value << "\", ";
        }
        str << " } }";
        return str;
    }
};

struct TPutResponse {
    TVector<TKeyValue> PrevKVs;

    friend IOutputStream& operator<<(IOutputStream& str, const TPutResponse& data) {
        str << "[TPutResponse] { "
            << "PrevKVs: {";

        for (const auto& kv : data.PrevKVs) {
            str << kv << ", ";
        }

        str << " } }";
        return str;
    }

    [[nodiscard]] constexpr bool IsWrite() const noexcept {
        return true;
    }
};

struct TDeleteRangeRequest {
    TString Key;
    TString RangeEnd;
    bool PrevKV;
    
    friend IOutputStream& operator<<(IOutputStream& str, const TDeleteRangeRequest& data) {
        str << "[TDeleteRangeRequest] { "
            << "Key: \"" << data.Key << "\", "
            << "RangeEnd: \"" << data.RangeEnd << "\", "
            << "PrevKV: " << data.PrevKV
            << " }";
        return str;
    }
};

struct TDeleteRangeResponse {
    size_t Deleted;
    TVector<TKeyValue> PrevKVs;

    friend IOutputStream& operator<<(IOutputStream& str, const TDeleteRangeResponse& data) {
        str << "[TDeleteRangeResponse] { "
            << "Deleted: " << data.Deleted << ", "
            << "PrevKVs: { ";

        for (const auto& kv : data.PrevKVs) {
            str << kv << ", ";
        }
        str << " } }";
        return str;
    }

    [[nodiscard]] constexpr bool IsWrite() const noexcept {
        return Deleted > 0;
    }
};

struct TTxnRequest;

using TRequestOp = std::variant<
    std::shared_ptr<TRangeRequest>,
    std::shared_ptr<TPutRequest>,
    std::shared_ptr<TDeleteRangeRequest>,
    std::shared_ptr<TTxnRequest>
>;

struct TTxnResponse;

using TResponseOp = std::variant<
    std::shared_ptr<TRangeResponse>,
    std::shared_ptr<TPutResponse>,
    std::shared_ptr<TDeleteRangeResponse>,
    std::shared_ptr<TTxnResponse>
>;

struct TTxnCompareRequest {
    enum class ECompareResult {
        EQUAL,
        GREATER,
        LESS,
        NOT_EQUAL,
    };
    ECompareResult Result;
    TMaybe<i64> Target_create_revision;
    TMaybe<i64> Target_mod_revision;
    TMaybe<i64> Target_version;
    TMaybe<TString> Target_value;
    TString Key;
    TString Range_end;

    friend IOutputStream& operator<<(IOutputStream& str, const TTxnCompareRequest& data) {
        str << "[TTxnCompareRequest] { "
            << "Result: " << std::underlying_type_t<ECompareResult>(data.Result) << ", ";
        if (data.Target_create_revision) {
            str << "Target_create_revision: " << *data.Target_create_revision << ", ";
        }
        if (data.Target_mod_revision) {
            str << "Target_mod_revision: " << *data.Target_mod_revision << ", ";
        }
        if (data.Target_version) {
            str << "Target_version: " << *data.Target_version << ", ";
        }
        if (data.Target_value) {
            str << "Target_value: " << *data.Target_value << ", ";
        }
        str << "Key: \"" << data.Key << "\", "
            << "Range_end: \"" << data.Range_end << "\" "
            << " }";
        return str;
    }
};

struct TTxnCompareResponse {
    bool Succeeded;

    friend IOutputStream& operator<<(IOutputStream& str, const TTxnCompareResponse& data) {
        str << "[TTxnCompareResponse] { "
            << "Succeeded: " << data.Succeeded
            << " }";
        return str;
    }
};

struct TTxnRequest {
    TVector<TTxnCompareRequest> Compare;
    std::array<TVector<TRequestOp>, 2> Requests;

    friend IOutputStream& operator<<(IOutputStream& str, const TTxnRequest& data) {
        str << "[TTxnRequest] { "
            << "Compare: { ";
        for (const auto& c : data.Compare) {
            str << c << ", ";
        }
        str << " }"
            << "Requests: ";
        for (const auto& Requests : data.Requests) {
            str << "{ ";
            for (const auto& req : Requests) {
                str << "{ ";
                std::visit([&str](auto&& arg) {
                    str << *arg << ", ";
            }, req);
            str << " } ";
            }
            str << " }, ";
        }
        str << " }";
        return str;
    }
};

struct TTxnResponse {
    bool Succeeded;
    TVector<TResponseOp> Responses;

    friend IOutputStream& operator<<(IOutputStream& str, const TTxnResponse& data) {
        str << "[TTxnResponse] { "
            << "Succeeded: " << data.Succeeded << ", "
            << "Responses: { ";
        for (const auto& resp : data.Responses) {
            str << "{ ";
            std::visit([&str](auto&& arg) {
                    str << *arg << ", ";
            }, resp);
            str << " }, ";
        }
        str << " } }";
        return str;
    }

    [[nodiscard]] constexpr bool IsWrite() const noexcept {
        return std::any_of(Responses.begin(), Responses.end(), [](const TResponseOp& resp) {
            return std::visit([](const auto& r) { return r->IsWrite(); }, resp);
        });
    }
};

// TODO [pavelbezpravel]: WIP.

struct TCompactionRequest {
    i64 Revision;
    bool Physical;

    friend IOutputStream& operator<<(IOutputStream& str, const TCompactionRequest& data) {
        str << "[TCompactionRequest] { "
            << "revision: " << data.Revision << ", "
            << "physical: " << data.Physical
            << " }";
        return str;
    }
};

struct TCompactionResponse {
    [[nodiscard]] constexpr bool IsWrite() const noexcept {
        return false;
    }

    friend IOutputStream& operator<<(IOutputStream& str, const TCompactionResponse& data) {
        Y_UNUSED(data);
        str << "[TCompactionResponse] {}";
        return str;
    }
};

} // namespace NYdb::NEtcd
