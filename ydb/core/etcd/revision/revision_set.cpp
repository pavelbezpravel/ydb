#include "revision_set.h"

#include "events.h"

#include <utility>

#include <ydb/core/base/path.h>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TRevisionSetActor : public TQueryBase {
public:
    TRevisionSetActor(ui64 logComponent, TString&& sessionId, TString&& path, NKikimr::TQueryBase::TTxControl txControl, TString&& txId, ui64 cookie, i64 revision, i64 compactRevision)
        : TQueryBase(logComponent, std::move(sessionId), path, path, txControl, std::move(txId), cookie, revision)
        , CompactRevision(compactRevision) {
            LOG_E("[TRevisionSetActor] TRevisionSetActor::TRevisionSetActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\"");
    }

    void OnRunQuery() override {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

            DECLARE $revision AS Int64;
            DECLARE $compact_revision AS Int64;

            $revision = AsList(
                AsStruct(FALSE AS id, $compact_revision AS revision),
                AsStruct(TRUE  AS id, $revision AS revision),
            );
            UPSERT
                INTO revision
                    SELECT *
                        FROM AS_TABLE($revision);
            SELECT * FROM revision;)"
        );

        NYdb::TParamsBuilder params;
        params
            .AddParam("$revision")
                .Int64(Revision)
                .Build()
            .AddParam("$compact_revision")
                .Int64(CompactRevision)
                .Build();

        RunDataQuery(query, &params, TxControl);
    }

    void OnQueryResult() override {
        Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

        NYdb::TResultSetParser parser(ResultSets[0]);

        Y_ABORT_UNLESS(parser.RowsCount() == 2, "Expected 2 rows in database response");

        while (parser.TryNextRow()) {
            bool id = *parser.ColumnParser("id").GetOptionalBool();
            auto rev = *parser.ColumnParser("revision").GetOptionalInt64();
            if (id) {
                Revision = rev;
            } else {
                CompactRevision = rev;
            }
        }

        DeleteSession = TxControl.Commit;

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        LOG_E("[TRevisionSetActor] TRevisionSetActor::OnFinish(); Revision: " << Revision);
        Send(Owner, new TEvEtcdRevision::TEvRevisionResponse(status, std::move(issues), SessionId, TxId, Revision, CompactRevision), {}, Cookie);
    }

private:
    i64 CompactRevision;
};

} // anonymous namespace

NActors::IActor* CreateRevisionSetActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, ui64 cookie, i64 revision, i64 compactRevision) {
    return new TRevisionSetActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), cookie, revision, compactRevision);
}

} // namespace NYdb::NEtcd