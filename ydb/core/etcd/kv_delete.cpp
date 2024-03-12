#include "kv_delete.h"
#include "events.h"

#include <utility>
#include <ydb/core/base/path.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

TKvDeleteActor::TKvDeleteActor(ui64 logComponent, TString sessionId, TString path, i64 currentRevision, TDeleteRequest deleteRequest)
    : NKikimr::TQueryBase(logComponent, sessionId, NKikimr::JoinPath({path, "kv"}))
    , Path(std::move(path))
    , CurrentRevision(currentRevision)
    , DeleteRequest(std::move(deleteRequest))
{}

void TKvDeleteActor::OnRunQuery() {
    TStringBuilder query;
    query << Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        DECLARE $revision AS Int64;
        DECLARE $key AS String;
        DECLARE $range_end AS String;

        $prev_kv = (
            SELECT *
                FROM kv
                WHERE key BETWEEN $key AND $range_end
                    AND delete_revision IS NULL
        );
        UPSERT
            INTO kv (key, mod_revision, delete_revision)
            SELECT key, mod_revision, $revision
                FROM $prev_kv;
    )", Path.c_str());

    if (DeleteRequest.PrevKv) {
        query << R"(
        SELECT * FROM $prev_kv;
        )";
    } else {
        query << R"(
        SELECT COUNT(*) AS result FROM $prev_kv;
        )";
    }

    NYdb::TParamsBuilder params;
    params
        .AddParam("$revision")
            .Int64(CurrentRevision)
            .Build()
        .AddParam("$key")
            .String(DeleteRequest.Key)
            .Build()
        .AddParam("$range_end")
            .String(DeleteRequest.RangeEnd)
            .Build()
        .Build();

    RunDataQuery(query, &params);
}

void TKvDeleteActor::OnQueryResult() {
    if (ResultSets.size() != 1) {
        Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
        return;
    }

    if (DeleteRequest.PrevKv) {
        NYdb::TResultSetParser parser(ResultSets[0]);

        DeleteResponse.Deleted = parser.RowsCount();

        DeleteResponse.PrevKvs.reserve(parser.RowsCount());
        while (parser.TryNextRow()) {
            TKeyValue kv {
                .key = parser.ColumnParser("key").GetString(),
                .create_revision = parser.ColumnParser("create_revision").GetInt64(),
                .mod_revision = parser.ColumnParser("mod_revision").GetInt64(),
                .version = parser.ColumnParser("version").GetInt64(),
                .value = parser.ColumnParser("value").GetString(),
            };
            DeleteResponse.PrevKvs.emplace_back(std::move(kv));
        }
    } else {
        NYdb::TResultSetParser parser(ResultSets[0]);

        if (parser.RowsCount() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Expected 1 row in database response");
            return;
        }
        
        parser.TryNextRow();

        DeleteResponse.Deleted = parser.ColumnParser("result").GetUint64();
    }
    
    Finish();
}

void TKvDeleteActor::OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
    Send(Owner, new TEvPrivate::TEvDeleteResponse(status, std::move(issues), std::move(DeleteResponse)));
}

} // namespace NYdb::NEtcd
