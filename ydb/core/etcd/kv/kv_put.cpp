#include "kv_put.h"

#include "events.h"
#include "proto.h"

#include <utility>

#include <ydb/core/base/path.h>
#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TKVPutActor : public TQueryBase {
public:
    TKVPutActor(ui64 logComponent, TString&& sessionId, TString&& path, TTxControl txControl, TString&& txId, i64 revision, uint64_t cookie, TPutRequest&& request)
        : TQueryBase(logComponent, std::move(sessionId), NKikimr::JoinPath({path, "kv"}), std::move(path), txControl, std::move(txId))
        , Revision(revision)
        , Cookie(cookie)
        , Request(request) {
    }

    void OnRunQuery() override {
        TStringBuilder query;
        query << Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

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
        Request.IgnoreValue ? R"(ENSURE(value, value IS NOT NULL, "value for key " || key || " is absent"))" : "new_value"
        );

        if (Request.PrevKV) {
            query << R"(
            SELECT * FROM $prev_kv;
            )";
        }

        NYdb::TParamsBuilder params;
        params
            .AddParam("$revision")
                .Int64(Revision)
                .Build();

        auto& newKVParam = params.AddParam("$new_kv");
        newKVParam.BeginList();
        for (const auto& [key, value] : Request.KVs) {
            newKVParam.AddListItem()
                .BeginStruct()
                .AddMember("key")
                    .String(key)
                .AddMember("new_value")
                    .String(value)
                .EndStruct();
        }
        newKVParam.EndList();
        newKVParam.Build();

        RunDataQuery(query, &params, TxControl);
    }

    void OnQueryResult() override {
        if (Request.PrevKV) {
            if (ResultSets.size() != 1) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
                return;
            }

            NYdb::TResultSetParser parser(ResultSets[0]);

            Response.PrevKVs.reserve(parser.RowsCount());
            while (parser.TryNextRow()) {
                TKeyValue kv {
                    .key = parser.ColumnParser("key").GetString(),
                    .create_revision = parser.ColumnParser("create_revision").GetInt64(),
                    .mod_revision = parser.ColumnParser("mod_revision").GetInt64(),
                    .version = parser.ColumnParser("version").GetInt64(),
                    .value = parser.ColumnParser("value").GetString(),
                };
                Response.PrevKVs.emplace_back(std::move(kv));
            }
        } else if (ResultSets.size() != 0) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvEtcdKV::TEvPutResponse(status, std::move(issues), TxId, std::move(Response)), {}, Cookie);
    }

private:
    i64 Revision;
    uint64_t Cookie;
    TPutRequest Request;
    TPutResponse Response;
};

} // anonymous namespace

NActors::IActor* CreateKVPutActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, i64 revision, uint64_t cookie, TPutRequest request) {
    return new TKVPutActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, cookie, std::move(request));
}

} // namespace NYdb::NEtcd
