#include "revision_set.h"

#include "events.h"

#include <utility>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TRevisionSetActor : public TQueryBase {
public:
    TRevisionSetActor(ui64 logComponent, TString&& sessionId, TString&& path, NKikimr::TQueryBase::TTxControl txControl, TString&& txId, i64 revision, i64 compactRevision)
        : TQueryBase(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, compactRevision) {
        LOG_D("[TRevisionSetActor] TRevisionSetActor::TRevisionSetActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\"");
    }

    void OnRunQuery() override {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

            DECLARE $revision AS Int64;
            DECLARE $compact_revision AS Int64;

            UPSERT INTO revision (id, revision) VALUES
                (FALSE, $compact_revision),
                (TRUE,  $revision);)"
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
        Y_ABORT_UNLESS(ResultSets.empty(), "Unexpected database response");

        DeleteSession = false;

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        LOG_D("[TRevisionSetActor] TRevisionSetActor::OnFinish(); Revision: " << Revision << " CompactRevision: " << CompactRevision);
        Send(Owner, new TEvEtcdRevision::TEvRevisionResponse(status, std::move(issues), SessionId, TxId, Revision, CompactRevision));
    }
};

} // anonymous namespace

NActors::IActor* CreateRevisionSetActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, i64 revision, i64 compactRevision) {
    return new TRevisionSetActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, compactRevision);
}

} // namespace NYdb::NEtcd