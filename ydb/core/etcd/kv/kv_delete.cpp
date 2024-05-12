#include "kv_delete.h"

#include "events.h"
#include "proto.h"

#include <utility>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TKVDeleteActor : public TQueryBase {
public:
    TKVDeleteActor(ui64 logComponent, TString&& sessionId, TString&& path, TTxControl txControl, TString&& txId, i64 revision, i64 compactRevision, TDeleteRangeRequest&& request)
        : TQueryBase(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, compactRevision)
        , CommitTx(std::exchange(TxControl.Commit, false))
        , Request(request) {
        LOG_D("[TKVDeleteActor] TKVDeleteActor::TKVDeleteActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\" Request: " << request);
    }

    void OnRunQuery() override {
        auto compareCond = Compare(Request.Key, Request.RangeEnd);

        TStringBuilder query;
        query << Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

            DECLARE $revision AS Int64;
            DECLARE $key AS String;
            DECLARE $range_end AS String;

            $prev_kv = (
                SELECT *
                    FROM kv
                    WHERE %s
                        AND delete_revision IS NULL
            );
            UPSERT
                INTO kv (key, mod_revision, delete_revision)
                SELECT key, mod_revision, $revision
                    FROM $prev_kv;)",
            compareCond.c_str()
        );

        if (Request.PrevKV) {
            query << R"(
            SELECT * FROM $prev_kv;)";
        } else {
            query << R"(
            SELECT COUNT(*) AS result FROM $prev_kv;)";
        }

        NYdb::TParamsBuilder params;
        params
            .AddParam("$revision")
                .Int64(Revision + 1)
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
        Response.Revision = Revision;

        if (Request.PrevKV) {
            Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

            NYdb::TResultSetParser parser(ResultSets[0]);

            Response.Deleted = parser.RowsCount();

            Response.PrevKVs.reserve(Response.Deleted);
            while (parser.TryNextRow()) {
                TKeyValue kv{
                    .Key = std::move(*parser.ColumnParser("key").GetOptionalString()),
                    .ModRevision = *parser.ColumnParser("mod_revision").GetOptionalInt64(),
                    .CreateRevision = *parser.ColumnParser("create_revision").GetOptionalInt64(),
                    .Version = *parser.ColumnParser("version").GetOptionalInt64(),
                    .Value = std::move(*parser.ColumnParser("value").GetOptionalString()),
                };
                Response.PrevKVs.emplace_back(std::move(kv));
            }
        } else {
            Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

            NYdb::TResultSetParser parser(ResultSets[0]);

            Y_ABORT_UNLESS(parser.RowsCount() == 1, "Expected 1 row in database response");

            parser.TryNextRow();

            Response.Deleted = parser.ColumnParser("result").GetUint64();
        }

        DeleteSession = false;

        if (CommitTx && !Response.IsWrite()) {
            CommitTransaction();
            return;
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        LOG_D("[TKVDeleteActor] TKVDeleteActor::OnFinish(); Response: " << Response);
        Send(Owner, new TEvEtcdKV::TEvDeleteRangeResponse(status, std::move(issues), SessionId, TxId, std::move(Response)));
    }

private:
    bool CommitTx;
    TDeleteRangeRequest Request;
    TDeleteRangeResponse Response;
};

} // anonymous namespace

NActors::IActor* CreateKVQueryActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, i64 revision, i64 compactRevision, TDeleteRangeRequest request) {
    return new TKVDeleteActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, compactRevision, std::move(request));
}

} // namespace NYdb::NEtcd
