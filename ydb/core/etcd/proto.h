#pragma once

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
  std::unique_ptr<TRangeRequest>,
  std::unique_ptr<TPutRequest>,
  std::unique_ptr<TDeleteRequest>,
  std::unique_ptr<TTxnRequest>
>;

struct TTxnResponse;

using TResponseOp = std::variant<
  std::unique_ptr<TRangeResponse>,
  std::unique_ptr<TPutResponse>,
  std::unique_ptr<TDeleteResponse>,
  std::unique_ptr<TTxnResponse>
>;

struct TTxnCompareRequest {
  enum class ECompareResult {
    EQUAL,
    GREATER,
    LESS,
    NOT_EQUAL,
  };
  ECompareResult result;
  TMaybe<i64> target_create_revision;
  TMaybe<i64> target_mod_revision;
  TMaybe<i64> target_version;
  TMaybe<TString> target_value;
  TString key;
  TString range_end;
};

struct TTxnCompareResponse {
  bool succeeded;
};

struct TTxnRequest {
  TVector<TTxnCompareRequest> compare;
  TVector<TRequestOp> success;
  TVector<TRequestOp> failure;
};

struct TTxnResponse {
  bool succeeded;
  TVector<TResponseOp> responses;
};

} // namespace NYdb::NEtcd
