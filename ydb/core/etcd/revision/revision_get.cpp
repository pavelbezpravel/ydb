#include "revision_get.h"

#include "events.h"

#include <utility>

#include <ydb/core/base/path.h>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TRevisionGetActor : public TQueryBase {
public:
    TRevisionGetActor(ui64 logComponent, TString&& sessionId, TString&& path, NKikimr::TQueryBase::TTxControl txControl, TString&& txId, ui64 cookie)
        : TQueryBase(logComponent, std::move(sessionId), path, path, txControl, std::move(txId), cookie, {}) {
    }

    void OnRunQuery() override {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("%s");

            SELECT * FROM revision;)",
            Path.c_str()
        );

        RunDataQuery(query, nullptr, TxControl);
    }

    void OnQueryResult() override {
        Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

        NYdb::TResultSetParser parser(ResultSets[0]);
        
        Y_ABORT_UNLESS(parser.RowsCount() == 2, "Expected 2 rows in database response");

        while (parser.TryNextRow()) {
            bool id = parser.ColumnParser("id").GetBool();
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
        Send(Owner, new TEvEtcdRevision::TEvRevisionResponse(status, std::move(issues), SessionId, TxId, Revision, CompactRevision), {}, Cookie);
    }

private:
    i64 CompactRevision;
};

} // anonymous namespace

NActors::IActor* CreateRevisionGetActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, ui64 cookie) {
    return new TRevisionGetActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), cookie);
}

} // namespace NYdb::NEtcd
