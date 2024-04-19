#include "kv_delete.h"

#include "events.h"
#include "proto.h"

#include <utility>

#include <ydb/core/base/path.h>
#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TKVDeleteActor : public TQueryBase {
public:
    TKVDeleteActor(ui64 logComponent, TString&& sessionId, TString&& path, TTxControl txControl, TString&& txId, i64 revision, uint64_t cookie, TDeleteRangeRequest&& request)
        : TQueryBase(logComponent, std::move(sessionId), NKikimr::JoinPath({path, "kv"}), std::move(path), txControl, std::move(txId))
        , Revision(revision)
        , Cookie(cookie)
        , Request(request) {
    }

    void OnRunQuery() override {
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
                    FROM $prev_kv;)",
            Path.c_str()
        );

        if (Request.PrevKV) {
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
                .Int64(Revision)
                .Build()
            .AddParam("$key")
                .String(Request.Key)
                .Build()
            .AddParam("$range_end")
                .String(Request.RangeEnd)
                .Build();

        RunDataQuery(query, &params, TxControl);
    }

    void OnQueryResult() override {
        if (Request.PrevKV) {
            if (ResultSets.size() != 1) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
                return;
            }

            NYdb::TResultSetParser parser(ResultSets[0]);

            Response.Deleted = parser.RowsCount();

            Response.PrevKVs.reserve(parser.RowsCount());
            while (parser.TryNextRow()) {
                TKeyValue kv{
                    .key = parser.ColumnParser("key").GetString(),
                    .create_revision = parser.ColumnParser("create_revision").GetInt64(),
                    .mod_revision = parser.ColumnParser("mod_revision").GetInt64(),
                    .version = parser.ColumnParser("version").GetInt64(),
                    .value = parser.ColumnParser("value").GetString(),
                };
                Response.PrevKVs.emplace_back(std::move(kv));
            }
        } else {
            if (ResultSets.size() != 1) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
                return;
            }

            NYdb::TResultSetParser parser(ResultSets[0]);

            if (parser.RowsCount() != 1) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Expected 1 row in database response");
                return;
            }

            parser.TryNextRow();

            Response.Deleted = parser.ColumnParser("result").GetUint64();
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvEtcdKV::TEvDeleteRangeResponse(status, std::move(issues), TxId, std::move(Response)), {}, Cookie);
    }

private:
    i64 Revision;
    uint64_t Cookie;
    TDeleteRangeRequest Request;
    TDeleteRangeResponse Response;
};

} // anonymous namespace

NActors::IActor* CreateKVDeleteActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, i64 revision, uint64_t cookie, TDeleteRangeRequest request) {
    return new TKVDeleteActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, cookie, std::move(request));
}

} // namespace NYdb::NEtcd
