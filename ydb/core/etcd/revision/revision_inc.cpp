#include "revision_inc.h"

#include "events.h"

#include <utility>

#include <ydb/core/base/path.h>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TRevisionIncActor : public TQueryBase {
public:
    TRevisionIncActor(ui64 logComponent, TString&& sessionId, TString&& path, NKikimr::TQueryBase::TTxControl txControl, TString&& txId, ui64 cookie)
        : TQueryBase(logComponent, std::move(sessionId), path, path, txControl, std::move(txId), cookie, {}) {
            LOG_E("[TRevisionIncActor] TRevisionIncActor::TRevisionIncActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\"");
    }

    void OnRunQuery() override {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

            $revision = (
                SELECT *
                    FROM revision
                    LIMIT 1
            );
            UPSERT
                INTO revision
                SELECT
                        id,
                        revision + 1 AS revision,
                    FROM $revision;
            SELECT revision FROM $revision;)"
        );

        RunDataQuery(query, nullptr, TxControl);
    }

    void OnQueryResult() override {
        Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

        NYdb::TResultSetParser parser(ResultSets[0]);

        parser.TryNextRow();

        Revision = *parser.ColumnParser("revision").GetOptionalInt64();

        DeleteSession = TxControl.Commit;

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        LOG_E("[TRevisionIncActor] TRevisionIncActor::OnFinish(); Revision: " << Revision);
        Send(Owner, new TEvEtcdRevision::TEvRevisionResponse(status, std::move(issues), SessionId, TxId, Revision), {}, Cookie);
    }
};

} // anonymous namespace

NActors::IActor* CreateRevisionIncActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, ui64 cookie) {
    return new TRevisionIncActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), cookie);
}

} // namespace NYdb::NEtcd
