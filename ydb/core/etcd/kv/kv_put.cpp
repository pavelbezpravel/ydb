#include "kv_put.h"

#include "events.h"
#include "proto.h"

#include <utility>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TKVPutActor : public TQueryBase {
public:
    TKVPutActor(ui64 logComponent, TString&& sessionId, TString&& path, TTxControl txControl, TString&& txId, i64 revision, i64 compactRevision, TPutRequest&& request)
        : TQueryBase(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, compactRevision)
        , Request(request) {
        TxControl.Commit = false;
        LOG_D("[TKVPutActor] TKVPutActor::TKVPutActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\" Request: " << request);
    }

    void OnRunQuery() override {
        if (!Request.IgnoreValue) {
            OnRunPutQuery();
            return;
        }

        TString query = R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

            DECLARE $key AS String;

            SELECT
                    value
                FROM kv
                WHERE key == $key;)";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$key")
                .String(Request.Key)
                .Build();

        RunDataQuery(query, &params, TxControl);
    }

    void OnQueryResult() override {
        Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

        NYdb::TResultSetParser parser(ResultSets[0]);

        if (parser.RowsCount() < 1) {
            Finish(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssues{});
            return;
        }

        Y_ABORT_UNLESS(parser.RowsCount() == 1, "Expected 1 row in database response");

        parser.TryNextRow();

        Request.Value = std::move(*parser.ColumnParser("value").GetOptionalString());

        OnRunPutQuery();
    }

    void OnRunPutQuery() {
        TStringBuilder query;
        query << Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

            DECLARE $revision AS Int64;
            DECLARE $key AS String;
            DECLARE $new_value AS String;

            $prev_kv = (
                SELECT *
                    FROM kv
                    WHERE key == $key
            );
            UPSERT
                INTO kv_past
                SELECT
                        key,
                        UNWRAP(mod_revision) AS mod_revision,
                        create_revision,
                        version,
                        $revision as delete_revision,
                        value,
                    FROM $prev_kv;
            UPSERT
                INTO kv (key, mod_revision, create_revision, version, value) VALUES
                ($key, $revision, $revision, 1, $new_value);
            UPSERT
                INTO kv
                SELECT
                        key,
                        create_revision,
                        version + 1 AS version,
                    FROM $prev_kv;)"
        );

        if (Request.PrevKV) {
            query << R"(
            SELECT * FROM $prev_kv;)";
        }

        NYdb::TParamsBuilder params;
        params
            .AddParam("$revision")
                .Int64(Revision + 1)
                .Build()
            .AddParam("$key")
                .String(Request.Key)
                .Build()
            .AddParam("$new_value")
                .String(Request.Value)
                .Build();

        SetQueryResultHandler(&TKVPutActor::OnPutQueryResult);
        RunDataQuery(query, &params, TxControl);
    }

    void OnPutQueryResult() {
        Response.Revision = Revision;

        if (Request.PrevKV) {
            Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

            NYdb::TResultSetParser parser(ResultSets[0]);

            Y_ABORT_UNLESS(parser.RowsCount() <= 1, "Expected 0 or 1 row in database response");

            while (parser.TryNextRow()) {
                Response.PrevKV = {
                    .Key = std::move(*parser.ColumnParser("key").GetOptionalString()),
                    .ModRevision = *parser.ColumnParser("mod_revision").GetOptionalInt64(),
                    .CreateRevision = *parser.ColumnParser("create_revision").GetOptionalInt64(),
                    .Version = *parser.ColumnParser("version").GetOptionalInt64(),
                    .Value = std::move(*parser.ColumnParser("value").GetOptionalString()),
                };
            }
        } else {
            Y_ABORT_UNLESS(ResultSets.empty(), "Unexpected database response");
        }

        DeleteSession = false;

        Y_ABORT_UNLESS(Response.IsWrite());

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        LOG_D("[TKVPutActor] TKVPutActor::OnFinish(); Owner: " << Owner << ", Response: " << Response << ", Issues: \"" << issues.ToString() << "\" Status: " << status);
        if (status == Ydb::StatusIds::PRECONDITION_FAILED) {
            auto errMessage = NYql::TIssue{"etcdserver: key not found"};
            issues.Clear();
            issues.AddIssues({errMessage});
        }
        Send(Owner, new TEvEtcdKV::TEvPutResponse(status, std::move(issues), SessionId, TxId, std::move(Response)));
    }

private:
    TPutRequest Request;
    TPutResponse Response{};
};

} // anonymous namespace

NActors::IActor* CreateKVQueryActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, i64 revision, i64 compactRevision, TPutRequest request) {
    return new TKVPutActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, compactRevision, std::move(request));
}

} // namespace NYdb::NEtcd
