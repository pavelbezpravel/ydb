#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include "ydb/library/yql/public/issue/yql_issue.h"
#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

#include <ydb/public/api/etcd/api/etcdserverpb/rpc.grpc.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NEtcd {

// TODO [pavelbezpravel]: test.

struct TKeyValue {
    TString key;
    i64 create_revision;
    i64 mod_revision;
    i64 version;
    TString value;
};

struct TRangeResponse {
    TVector<TKeyValue> Kvs;
    bool More;
    size_t Count;
};

struct TEvEtcd {
    enum EEv {
        EvRangeResponse = EventSpaceBegin(NKikimr::TKikimrEvents::ES_ETCD),

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::TKikimrEvents::ES_ETCD),
        "expect EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_ETCD)");

    struct TEvRangeResponse : public NActors::TEventLocal<TEvRangeResponse, EvRangeResponse> {
        TEvRangeResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TRangeResponse&& rangeResponse)
            : Status(status)
            , Issues(issues)
            , RangeResponse(rangeResponse)
        {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TRangeResponse RangeResponse;
    };

};

} // namespace NKikimr::NEtcd