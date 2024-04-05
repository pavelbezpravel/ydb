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
    i64 create_revision;
    i64 mod_revision;
    i64 version;
    TString value;
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
};

struct TRangeResponse {
    TVector<TKeyValue> Kvs;
    bool More;
    size_t Count;
};

struct TPutRequest {
    TVector<std::pair<TString, TString>> Kvs;
    bool PrevKv;
    bool IgnoreValue;
};

struct TPutResponse {
    TVector<TKeyValue> PrevKvs;
};

struct TDeleteRequest {
    TString Key;
    TString RangeEnd;
    bool PrevKv;
};

struct TDeleteResponse {
    size_t Deleted;
    TVector<TKeyValue> PrevKvs;
};

struct TTxnRequest;

using TRequestOp = std::variant<
    std::shared_ptr<TRangeRequest>,
    std::shared_ptr<TPutRequest>,
    std::shared_ptr<TDeleteRequest>,
    std::shared_ptr<TTxnRequest>
>;

struct TTxnResponse;

using TResponseOp = std::variant<
    std::shared_ptr<TRangeResponse>,
    std::shared_ptr<TPutResponse>,
    std::shared_ptr<TDeleteResponse>,
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
};

struct TTxnCompareResponse {
    bool Succeeded;
};

struct TTxnRequest {
    TVector<TTxnCompareRequest> Compare;
    std::array<TVector<TRequestOp>, 2> Requests;
};

struct TTxnResponse {
    bool Succeeded;
    TVector<TResponseOp> Responses;
};

} // namespace NYdb::NEtcd
