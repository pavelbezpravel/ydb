#pragma once

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NYdb::NEtcd {

struct TEvEtcdRevision {
    // Event ids
    enum EEv : ui32 {
        EvCreateTableResponse = EventSpaceBegin(NKikimr::TKikimrEvents::ES_ETCD_REVISION),
        EvRevisionResponse,

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_ETCD_REVISION),
        "expect EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_ETCD_REVISION)"
    );

    // Events

    struct TEvCreateTableResponse : public NActors::TEventLocal<TEvCreateTableResponse, EvCreateTableResponse> {};

    struct TEvRevisionResponse : public NActors::TEventLocal<TEvRevisionResponse, EvRevisionResponse> {
        TEvRevisionResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString sessionId, TString txId, i64 revision, i64 compactRevision)
            : Status(status)
            , Issues(issues)
            , SessionId(std::move(sessionId))
            , TxId(std::move(txId))
            , Revision(revision)
            , CompactRevision(compactRevision) {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString SessionId;
        TString TxId;
        i64 Revision;
        i64 CompactRevision;
    };
};

} // namespace NYdb::NEtcd
