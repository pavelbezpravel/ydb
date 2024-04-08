#pragma once

#include "proto.h"

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NYdb::NEtcd {

struct TEvEtcdKV {
    // Event ids
    enum EEv : ui32 {
        EvCreateTableResponse = EventSpaceBegin(NKikimr::TKikimrEvents::ES_ETCD_KV),
        EvRangeResponse,
        EvPutResponse,
        EvDeleteRangeResponse,
        EvTxnCompareResponse,
        EvTxnResponse,
        EvCompactionResponse,

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_ETCD_KV),
        "expect EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_ETCD_KV)"
    );

    // Events
    struct TEvCreateTableResponse : public NActors::TEventLocal<TEvCreateTableResponse, EvCreateTableResponse> {
    };

    struct TEvRangeResponse : public NActors::TEventLocal<TEvRangeResponse, EvRangeResponse> {
        TEvRangeResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString txId, TRangeResponse&& response)
            : Status(status)
            , Issues(issues)
            , TxId(std::move(txId))
            , Response(response)
        {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString TxId;
        TRangeResponse Response;
    };

    struct TEvPutResponse : public NActors::TEventLocal<TEvPutResponse, EvPutResponse> {
        TEvPutResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString txId, TPutResponse&& response)
            : Status(status)
            , Issues(issues)
            , TxId(std::move(txId))
            , Response(response)
        {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString TxId;
        TPutResponse Response;
    };

    struct TEvDeleteRangeResponse : public NActors::TEventLocal<TEvDeleteRangeResponse, EvDeleteRangeResponse> {
        TEvDeleteRangeResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString txId, TDeleteRangeResponse&& response)
            : Status(status)
            , Issues(issues)
            , TxId(std::move(txId))
            , Response(response)
        {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString TxId;
        TDeleteRangeResponse Response;
    };

    struct TEvTxnCompareResponse : public NActors::TEventLocal<TEvTxnCompareResponse, EvTxnCompareResponse> {
        TEvTxnCompareResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString txId, TTxnCompareResponse&& response)
            : Status(status)
            , Issues(issues)
            , TxId(std::move(txId))
            , Response(response)
        {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString TxId;
        TTxnCompareResponse Response;
    };

    struct TEvTxnResponse : public NActors::TEventLocal<TEvTxnResponse, EvTxnResponse> {
        TEvTxnResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString txId, TTxnResponse&& response)
            : Status(status)
            , Issues(issues)
            , TxId(std::move(txId))
            , Response(response)
        {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString TxId;
        TTxnResponse Response;
    };

    struct TEvCompactionResponse : public NActors::TEventLocal<TEvCompactionResponse, EvCompactionResponse> {
        TEvCompactionResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString txId, TCompactionResponse&& response)
            : Status(status)
            , Issues(issues)
            , TxId(std::move(txId))
            , Response(response)
        {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString TxId;
        TCompactionResponse Response;
    };
};

} // namespace NYdb::NEtcd
