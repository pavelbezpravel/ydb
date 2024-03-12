#pragma once

#pragma once

#include "proto.h"
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

namespace NYdb::NEtcd {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvRangeResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPutResponse,
        EvDeleteResponse,
        EvTxnCompareResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
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

    struct TEvPutResponse : public NActors::TEventLocal<TEvPutResponse, EvPutResponse> {
        TEvPutResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TPutResponse&& putResponse)
            : Status(status)
            , Issues(issues)
            , PutResponse(putResponse)
        {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TPutResponse PutResponse;
    };

    struct TEvDeleteResponse : public NActors::TEventLocal<TEvDeleteResponse, EvDeleteResponse> {
        TEvDeleteResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TDeleteResponse&& deleteResponse)
            : Status(status)
            , Issues(issues)
            , DeleteResponse(deleteResponse)
        {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TDeleteResponse DeleteResponse;
    };

    struct TEvTxnCompareResponse : public NActors::TEventLocal<TEvTxnCompareResponse, EvTxnCompareResponse> {
        TEvTxnCompareResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TTxnCompareResponse&& txnCompareResponse)
            : Status(status)
            , Issues(issues)
            , TxnCompareResponse(txnCompareResponse)
        {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TTxnCompareResponse TxnCompareResponse;
    };
};

} // namespace NYdb::NEtcd

