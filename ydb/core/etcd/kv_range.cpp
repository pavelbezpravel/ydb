#include "kv_range.h"
#include "events.h"

#include <utility>
#include <ydb/core/base/path.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

TKvRangeActor::TKvRangeActor(ui64 logComponent, TString sessionId, TString path, TRangeRequest rangeRequest)
    : NKikimr::TQueryBase(logComponent, sessionId, NKikimr::JoinPath({path, "kv"}))
    , Path(std::move(path))
    , RangeRequest(std::move(rangeRequest))
{}

void TKvRangeActor::OnRunQuery() {
    TStringBuilder query;
    query << Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        DECLARE $revision AS Int64;
        DECLARE $key AS String;
        DECLARE $range_end AS String;
        DECLARE $min_create_revision AS Int64;
        DECLARE $max_create_revision AS Int64;
        DECLARE $min_mod_revision AS Int64;
        DECLARE $max_mod_revision AS Int64;
        DECLARE $limit AS Int64;

        SELECT *, COUNT(*) OVER() AS count
            FROM kv
            WHERE key BETWEEN $key AND $range_end)",
        Path.c_str()
    );

    if (RangeRequest.revision > 0) {
        query << R"(
                AND create_revision <= $revision AND (delete_rev IS NULL OR $revision <= delete_rev))";
    }

    if (RangeRequest.min_create_revision > 0) {
        query << R"(
                AND $min_create_revision <= create_revision)";
    }

    if (RangeRequest.max_create_revision > 0) {
        query << R"(
                AND create_revision <= $max_create_revision)";
    }

    if (RangeRequest.min_mod_revision > 0) {
        query << R"(
                AND $min_mod_revision <= mod_revision)";
    }

    if (RangeRequest.max_mod_revision > 0) {
        query << R"(
                AND mod_revision <= $max_mod_revision)";
    }

    if (RangeRequest.sort_order != TRangeRequest::ESortOrder::NONE) {
        TString order = [&]() {
            switch (RangeRequest.sort_order) {
                case TRangeRequest::ESortOrder::ASCEND:
                    return "ASC";
                case TRangeRequest::ESortOrder::DESCEND:
                    return "DESC";
                default:
                    throw std::runtime_error("Unknwon sort order");
            }
        }();

        TString target = [&]() {
            switch (RangeRequest.sort_target) {
                case TRangeRequest::ESortTarget::KEY:
                    return "key";
                case TRangeRequest::ESortTarget::CREATE:
                    return "create_revision";
                case TRangeRequest::ESortTarget::MOD:
                    return "mod_revision";
                case TRangeRequest::ESortTarget::VERSION:
                    return "version";
                case TRangeRequest::ESortTarget::VALUE:
                    return "value";
                default:
                    throw std::runtime_error("Unknwon sort target");
            }
        }();

        query << Sprintf(R"(
                ORDER BY %s %s)", target.c_str(), order.c_str());
    }

    if (RangeRequest.limit > 0) {
        query << R"(
            LIMIT $limit)";
    }

    query << ";";

    NYdb::TParamsBuilder params;
    params
        .AddParam("$revision")
            .Int64(RangeRequest.revision)
            .Build()
        .AddParam("$key")
            .String(RangeRequest.key)
            .Build()
        .AddParam("$range_end")
            .String(RangeRequest.range_end)
            .Build()
        .AddParam("$min_create_revision")
            .Int64(RangeRequest.min_create_revision)
            .Build()
        .AddParam("$max_create_revision")
            .Int64(RangeRequest.max_create_revision)
            .Build()
        .AddParam("$min_mod_revision")
            .Int64(RangeRequest.min_mod_revision)
            .Build()
        .AddParam("$max_mod_revision")
            .Int64(RangeRequest.max_mod_revision)
            .Build()
        .AddParam("$limit")
            .Int64(RangeRequest.limit + 1) // to fill TRangeResponse::more field
            .Build()
        .Build();

    RunDataQuery(query, &params);
}

void TKvRangeActor::OnQueryResult() {
    if (ResultSets.size() != 1) {
        Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
        return;
    }

    NYdb::TResultSetParser parser(ResultSets[0]);

    RangeResponse.Count = std::min(parser.RowsCount(), RangeRequest.limit);
    
    RangeResponse.More = parser.RowsCount() > RangeRequest.limit;

    RangeResponse.Kvs.reserve(RangeResponse.Count);
    while (RangeResponse.Kvs.size() < RangeResponse.Count) {
        parser.TryNextRow();

        TKeyValue kv {
            .key = parser.ColumnParser("key").GetString(),
            .create_revision = parser.ColumnParser("create_revision").GetInt64(),
            .mod_revision = parser.ColumnParser("mod_revision").GetInt64(),
            .version = parser.ColumnParser("version").GetInt64(),
            .value = parser.ColumnParser("value").GetString(),
        };
        RangeResponse.Kvs.emplace_back(std::move(kv));
    }

    Finish();
}

void TKvRangeActor::OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
    Send(Owner, new TEvPrivate::TEvRangeResponse(status, std::move(issues), std::move(RangeResponse)));
}

} // namespace NYdb::NEtcd
