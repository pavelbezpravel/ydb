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
        EvRangeRequest,
        EvRangeResponse,
        EvPutRequest,
        EvPutResponse,
        EvDeleteRangeRequest,
        EvDeleteRangeResponse,
        EvTxnCompareResponse,
        EvTxnRequest,
        EvTxnResponse,
        EvCompactionRequest,
        EvCompactionResponse,
        EvResponse,

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_ETCD_KV),
        "expect EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_ETCD_KV)"
    );

    // Events

    struct TEvCreateTableResponse : public NActors::TEventLocal<TEvCreateTableResponse, EvCreateTableResponse> {};

    struct TEvRangeRequest : public NActors::TEventLocal<TEvRangeRequest, EvRangeRequest> {
        TEvRangeRequest(TRangeRequest&& request)
            : Request(request) {
        }

        TRangeRequest Request;
    };

    struct TEvRangeResponse : public NActors::TEventLocal<TEvRangeResponse, EvRangeResponse> {
        TEvRangeResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString sessionId, TString txId, TRangeResponse&& response)
            : Status(status)
            , Issues(issues)
            , SessionId(std::move(sessionId))
            , TxId(std::move(txId))
            , Response(response) {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString SessionId;
        TString TxId;
        TRangeResponse Response;
    };

    struct TEvPutRequest : public NActors::TEventLocal<TEvPutRequest, EvPutRequest> {
        TEvPutRequest(TPutRequest&& request)
            : Request(request) {
        }

        TPutRequest Request;
    };

    struct TEvPutResponse : public NActors::TEventLocal<TEvPutResponse, EvPutResponse> {
        TEvPutResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString sessionId, TString txId, TPutResponse&& response)
            : Status(status)
            , Issues(issues)
            , SessionId(std::move(sessionId))
            , TxId(std::move(txId))
            , Response(response) {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString SessionId;
        TString TxId;
        TPutResponse Response;
    };

    struct TEvDeleteRangeRequest : public NActors::TEventLocal<TEvDeleteRangeRequest, EvDeleteRangeRequest> {
        TEvDeleteRangeRequest(TDeleteRangeRequest&& request)
            : Request(request) {
        }

        TDeleteRangeRequest Request;
    };

    struct TEvDeleteRangeResponse : public NActors::TEventLocal<TEvDeleteRangeResponse, EvDeleteRangeResponse> {
        TEvDeleteRangeResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString sessionId, TString txId, TDeleteRangeResponse&& response)
            : Status(status)
            , Issues(issues)
            , SessionId(std::move(sessionId))
            , TxId(std::move(txId))
            , Response(response) {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString SessionId;
        TString TxId;
        TDeleteRangeResponse Response;
    };

    struct TEvTxnRequest : public NActors::TEventLocal<TEvTxnRequest, EvTxnRequest> {
        TEvTxnRequest(TTxnRequest&& request)
            : Request(request) {
        }

        TTxnRequest Request;
    };

    struct TEvTxnCompareResponse : public NActors::TEventLocal<TEvTxnCompareResponse, EvTxnCompareResponse> {
        TEvTxnCompareResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString sessionId, TString txId, TTxnCompareResponse&& response)
            : Status(status)
            , Issues(issues)
            , SessionId(std::move(sessionId))
            , TxId(std::move(txId))
            , Response(response) {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString SessionId;
        TString TxId;
        TTxnCompareResponse Response;
    };

    struct TEvTxnResponse : public NActors::TEventLocal<TEvTxnResponse, EvTxnResponse> {
        TEvTxnResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString sessionId, TString txId, TTxnResponse&& response)
            : Status(status)
            , Issues(issues)
            , SessionId(std::move(sessionId))
            , TxId(std::move(txId))
            , Response(response) {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString SessionId;
        TString TxId;
        TTxnResponse Response;
    };

    struct TEvCompactionRequest : public NActors::TEventLocal<TEvCompactionRequest, EvCompactionRequest> {
        TEvCompactionRequest(TCompactionRequest&& request)
            : Request(request) {
        }

        TCompactionRequest Request;
    };

    struct TEvCompactionResponse : public NActors::TEventLocal<TEvCompactionResponse, EvCompactionResponse> {
        TEvCompactionResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString sessionId, TString txId, TCompactionResponse&& response)
            : Status(status)
            , Issues(issues)
            , SessionId(std::move(sessionId))
            , TxId(std::move(txId))
            , Response(response) {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString SessionId;
        TString TxId;
        TCompactionResponse Response;
    };

    struct TEvResponse : public NActors::TEventLocal<TEvResponse, EvResponse> {
        TEvResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, TString sessionId, TString txId, TVector<TPutResponse>&& responses)
            : Status(status)
            , Issues(issues)
            , SessionId(std::move(sessionId))
            , TxId(std::move(txId))
            , Responses(responses) {
        }

        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
        TString SessionId;
        TString TxId;
        TVector<TPutResponse> Responses;
    };
};

} // namespace NYdb::NEtcd
