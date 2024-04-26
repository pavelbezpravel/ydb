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
    TString Key;
    i64 ModRevision;
    i64 CreateRevision;
    i64 Version;
    TString Value;

    friend IOutputStream& operator<<(IOutputStream& str, const TKeyValue& data) {
        str << "{ "
            << "Key: \"" << data.Key << "\", "
            << "Value: \"" << data.Value << "\", "
            << "CreateRevision: " << data.CreateRevision << ", "
            << "ModRevision: " << data.ModRevision << ", "
            << "Version: " << data.Version
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
        VERSION,
        CREATE,
        MOD,
        VALUE,
    };

    TString Key;
    TString RangeEnd;
    size_t Limit;
    i64 Revision;
    ESortOrder SortOrder;
    ESortTarget SortTarget;
    bool Serializable;
    bool KeysOnly;
    bool CountOnly;
    i64 MinModRevision;
    i64 MaxModRevision;
    i64 MinCreateRevision;
    i64 MaxCreateRevision;

    friend IOutputStream& operator<<(IOutputStream& str, const TRangeRequest& data) {
        str << "[TRangeRequest] { "
            << "Key: \"" << data.Key << "\", "
            << "RangeEnd: \"" << data.RangeEnd << "\", "
            << "Limit: " << data.Limit << ", "
            << "Revision: " << data.Revision << ", "
            << "SortOrder: " << std::underlying_type_t<ESortOrder>(data.SortOrder) << ", "
            << "SortTarget: " << std::underlying_type_t<ESortTarget>(data.SortTarget) << ", "
            << "Serializable: " << data.Serializable << ", "
            << "KeysOnly: " << data.KeysOnly << ", "
            << "CountOnly: " << data.CountOnly << ", "
            << "MinModRevision: " << data.MinModRevision << ", "
            << "MaxModRevision: " << data.MaxModRevision << ", "
            << "MinCreateRevision: " << data.MinCreateRevision << ", "
            << "MaxCreateRevison: " << data.MaxCreateRevision
            << " }";
        return str;
    }
};

struct TRangeResponse {
    i64 Revision;
    TVector<TKeyValue> KVs;
    bool More;
    size_t Count;

    [[nodiscard]] constexpr bool IsWrite() const noexcept {
        return false;
    }

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
    i64 Revision;
    TVector<TKeyValue> PrevKVs;

    [[nodiscard]] constexpr bool IsWrite() const noexcept {
        return true;
    }

    friend IOutputStream& operator<<(IOutputStream& str, const TPutResponse& data) {
        str << "[TPutResponse] { "
            << "PrevKVs: {";

        for (const auto& kv : data.PrevKVs) {
            str << kv << ", ";
        }

        str << " } }";
        return str;
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
    i64 Revision;
    size_t Deleted;
    TVector<TKeyValue> PrevKVs;

    [[nodiscard]] constexpr bool IsWrite() const noexcept {
        return Deleted > 0;
    }

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
    TMaybe<i64> TargetCreateRevision;
    TMaybe<i64> TargetModRevision;
    TMaybe<i64> TargetVersion;
    TMaybe<TString> TargetValue;
    TString Key;
    TString RangeEnd;

    friend IOutputStream& operator<<(IOutputStream& str, const TTxnCompareRequest& data) {
        str << "[TTxnCompareRequest] { "
            << "Result: " << std::underlying_type_t<ECompareResult>(data.Result) << ", ";
        if (data.TargetCreateRevision) {
            str << "TargetCreateRevision: " << *data.TargetCreateRevision << ", ";
        }
        if (data.TargetModRevision) {
            str << "TargetModRevision: " << *data.TargetModRevision << ", ";
        }
        if (data.TargetVersion) {
            str << "TargetVersion: " << *data.TargetVersion << ", ";
        }
        if (data.TargetValue) {
            str << "TargetValue: " << *data.TargetValue << ", ";
        }
        str << "Key: \"" << data.Key << "\", "
            << "RangeEnd: \"" << data.RangeEnd << "\" "
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
    i64 Revision;
    bool Succeeded;
    TVector<TResponseOp> Responses;

    [[nodiscard]] constexpr bool IsWrite() const noexcept {
        return std::any_of(Responses.begin(), Responses.end(), [](const TResponseOp& resp) {
            return std::visit([](const auto& r) { return r->IsWrite(); }, resp);
        });
    }

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
};

struct TCompactionRequest {
    i64 Revision;
    bool Physical;

    friend IOutputStream& operator<<(IOutputStream& str, const TCompactionRequest& data) {
        str << "[TCompactionRequest] { "
            << "Revision: " << data.Revision << ", "
            << "Physical: " << data.Physical
            << " }";
        return str;
    }
};

struct TCompactionResponse {
    i64 Revision;

    [[nodiscard]] constexpr bool IsWrite() const noexcept {
        return true;
    }

    friend IOutputStream& operator<<(IOutputStream& str, const TCompactionResponse&) {
        str << "[TCompactionResponse] {}";
        return str;
    }
};

} // namespace NYdb::NEtcd
