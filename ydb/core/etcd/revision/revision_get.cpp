#include "revision_get.h"

#include "events.h"

#include <utility>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TRevisionGetActor : public TQueryBase {
public:
    TRevisionGetActor(ui64 logComponent, TString&& sessionId, TString&& path, NKikimr::TQueryBase::TTxControl txControl, TString&& txId)
        : TQueryBase(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), {}, {}) {
        LOG_D("[TRevisionGetActor] TRevisionGetActor::TRevisionGetActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\"");
    }

    void OnRunQuery() override {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

            SELECT * FROM revision;)"
        );

        RunDataQuery(query, nullptr, TxControl);
    }

    void OnQueryResult() override {
        Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

        NYdb::TResultSetParser parser(ResultSets[0]);
        
        Y_ABORT_UNLESS(parser.RowsCount() == 2, "Expected 2 rows in database response");

        while (parser.TryNextRow()) {
            auto& rev = *parser.ColumnParser("id").GetOptionalBool() ? Revision : CompactRevision;
            rev = *parser.ColumnParser("revision").GetOptionalInt64();
        }

        DeleteSession = false;

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        LOG_D("[TRevisionGetActor] TRevisionGetActor::OnFinish(); Revision: " << Revision << " CompactRevision: " << CompactRevision);
        Send(Owner, new TEvEtcdRevision::TEvRevisionResponse(status, std::move(issues), SessionId, TxId, Revision, CompactRevision));
    }
};

} // anonymous namespace

NActors::IActor* CreateRevisionGetActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId) {
    return new TRevisionGetActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId));
}

} // namespace NYdb::NEtcd
