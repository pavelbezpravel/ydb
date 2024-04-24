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
    TKVPutActor(ui64 logComponent, TString&& sessionId, TString&& path, TTxControl txControl, TString&& txId, ui64 cookie, i64 revision, TPutRequest&& request, bool isFirstRequest)
        : TQueryBase(logComponent, std::move(sessionId), path, path, txControl, std::move(txId), cookie, revision)
        , CommitTx(std::exchange(TxControl.Commit, false))
        , Request(request)
        , IsFirstRequest(isFirstRequest) {
            LOG_E("[TKVPutActor] TKVPutActor::TKVPutActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\" Request: " << request << " IsFirstRequest: " << IsFirstRequest);
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
                        COALESCE(version, 0) + 1 AS version,
                        NULL AS delete_revision,
                        %s AS value,
                    FROM $next_kv;)",
            Request.IgnoreValue ? R"(ENSURE(value, value IS NOT NULL, "value for key " || key || " is absent"))" : "new_value"
        );

        if (Request.PrevKV) {
            query << R"(
            SELECT * FROM $next_kv;)";
        }

        NYdb::TParamsBuilder params;
        params
            .AddParam("$revision")
                .Int64(Revision + !IsFirstRequest)
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
        Response.Revision = Revision;

        if (Request.PrevKV) {
            Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

            NYdb::TResultSetParser parser(ResultSets[0]);

            Response.PrevKVs.reserve(parser.RowsCount());
            while (parser.TryNextRow()) {
                auto mod_revision = parser.ColumnParser("mod_revision").GetOptionalInt64();
                if (!mod_revision) {
                    continue;
                }
                TKeyValue kv {
                    .Key = std::move(parser.ColumnParser("key").GetString()),
                    .ModRevision = *mod_revision,
                    .CreateRevision = *parser.ColumnParser("create_revision").GetOptionalInt64(),
                    .Version = *parser.ColumnParser("version").GetOptionalInt64(),
                    .Value = std::move(*parser.ColumnParser("value").GetOptionalString()),
                };
                Response.PrevKVs.emplace_back(std::move(kv));
            }
        } else {
            Y_ABORT_UNLESS(ResultSets.empty(), "Unexpected database response");
        }

        DeleteSession = IsFirstRequest || (CommitTx && !Response.IsWrite());

        if (DeleteSession) {
            CommitTransaction();
            return;
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        LOG_E("[TKVPutActor] TKVPutActor::OnFinish(); Response: " << Response);
        Send(Owner, new TEvEtcdKV::TEvPutResponse(status, std::move(issues), SessionId, TxId, std::move(Response)), {}, Cookie);
    }

private:
    bool CommitTx;
    TPutRequest Request;
    TPutResponse Response;
    bool IsFirstRequest;
};

} // anonymous namespace

NActors::IActor* CreateKVQueryActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, ui64 cookie, i64 revision, TPutRequest request, bool isFirstRequest) {
    return new TKVPutActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), cookie, revision, std::move(request), isFirstRequest);
}

} // namespace NYdb::NEtcd
