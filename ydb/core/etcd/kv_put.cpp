#include "kv_put.h"
#include "events.h"

#include <utility>
#include <ydb/core/base/path.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

TKvPutActor::TKvPutActor(ui64 logComponent, TString sessionId, TString path, i64 currentRevision, TPutRequest putRequest)
    : NKikimr::TQueryBase(logComponent, sessionId, NKikimr::JoinPath({path, "kv"}))
    , Path(std::move(path))
    , CurrentRevision(currentRevision)
    , PutRequest(std::move(putRequest))
{}

void TKvPutActor::OnRunQuery() {
    TStringBuilder query;
    query << Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        DECLARE $revision AS Int64;
        DECLARE $new_kv AS List<Struct<
            key: String,
            new_value: String
        >>;

        $prev_kv = (
            SELECT *
                FROM kv
                WHERE delete_revision IS NULL
        );
        $next_kv = (
            SELECT *
                FROM AS_TABLE($new_kv) AS next
                LEFT JOIN $prev_kv     AS prev USING(key)
        );
        UPSERT
            INTO kv
            SELECT
                    key,
                    UNWRAP(mod_revision) AS mod_revision,
                    $revision AS delete_revision,
                FROM $next_kv
                WHERE mod_revision IS NOT NULL;
        UPSERT
            INTO kv
            SELECT
                    key,
                    $revision AS mod_revision,
                    COALESCE(create_revision, $revision) AS create_revision,
                    COALESCE(version, -1) + 1 AS version,
                    NULL AS delete_revision,
                    %s AS value,
                FROM $next_kv;
    )",
    Path.c_str(),
    PutRequest.IgnoreValue ? R"(ENSURE(value, value IS NOT NULL, "value for key " || key || " is absent"))" : "new_value"
    );

    if (PutRequest.PrevKv) {
        query << R"(
        SELECT * FROM $prev_kv;
        )";
    }

    NYdb::TParamsBuilder params;
    params
        .AddParam("$revision")
            .Int64(CurrentRevision)
            .Build();
    
    auto& newKvParam = params.AddParam("$new_kv");
    newKvParam.BeginList();
    for (const auto& [key, value] : PutRequest.Kvs) {
        newKvParam.AddListItem()
            .BeginStruct()
            .AddMember("key")
                .String(key)
            .AddMember("new_value")
                .String(value)
            .EndStruct();
    }
    newKvParam.EndList();
    newKvParam.Build();
        
    params.Build();

    RunDataQuery(query, &params);
}

void TKvPutActor::OnQueryResult() {
    if (PutRequest.PrevKv) {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser parser(ResultSets[0]);

        PutResponse.PrevKvs.reserve(parser.RowsCount());
        while (parser.TryNextRow()) {
            TKeyValue kv {
                .key = parser.ColumnParser("key").GetString(),
                .create_revision = parser.ColumnParser("create_revision").GetInt64(),
                .mod_revision = parser.ColumnParser("mod_revision").GetInt64(),
                .version = parser.ColumnParser("version").GetInt64(),
                .value = parser.ColumnParser("value").GetString(),
            };
            PutResponse.PrevKvs.emplace_back(std::move(kv));
        }
    } else if (ResultSets.size() != 0) {
        Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
        return;
    }
    
    Finish();
}

void TKvPutActor::OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
    Send(Owner, new TEvPrivate::TEvPutResponse(status, std::move(issues), std::move(PutResponse)));
}

} // namespace NYdb::NEtcd
