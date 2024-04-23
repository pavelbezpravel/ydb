#include "revision_table_init.h"

#include "events.h"

#include <utility>

#include <ydb/core/base/path.h>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/table_creator/table_creator.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TRevisionTableInitActor : public TQueryBase {
public:
    TRevisionTableInitActor(ui64 logComponent, TString&& sessionId, TString path, uint64_t cookie)
        : TQueryBase(logComponent, std::move(sessionId), path, path, TTxControl::BeginAndCommitTx(), {}, cookie, {}) {
    }

    // TODO [pavelbezpravel]: fix prefix.
    void OnRunQuery() override {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

            $initial = AsList(
                AsStruct(FALSE AS id, Int64("1") AS revision),
            );
            UPSERT
                INTO revision
                SELECT *
                    FROM AS_TABLE($initial);
            SELECT revision FROM AS_TABLE($initial);)"
        );

        RunDataQuery(query, nullptr, TxControl);
    }

    void OnQueryResult() override {
        Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

        NYdb::TResultSetParser parser(ResultSets[0]);
        parser.TryNextRow();

        Revision = parser.ColumnParser("revision").GetInt64();

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvEtcdRevision::TEvRevisionResponse(status, std::move(issues), SessionId, TxId, Revision), {}, Cookie);
    }
};

} // anonymous namespace

NActors::IActor* CreateRevisionTableInitActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie) {
    return new TRevisionTableInitActor(logComponent, std::move(sessionId), std::move(path), cookie);
}

} // namespace NYdb::NEtcd
