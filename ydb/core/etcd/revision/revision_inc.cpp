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
    TRevisionIncActor(ui64 logComponent, TString&& sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, uint64_t cookie)
        : TQueryBase(logComponent, std::move(sessionId), NKikimr::JoinPath({path, "revision"}), std::move(path), txControl)
        , Cookie(cookie) {
    }

    void OnRunQuery() override {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("%s");

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
            SELECT revision FROM $revision;
        )", Path.c_str());

        RunDataQuery(query, nullptr, TxControl);
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser parser(ResultSets[0]);
        parser.TryNextRow();

        Revision = parser.ColumnParser("revision").GetInt64();

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvEtcdRevision::TEvRevisionResponse(status, std::move(issues), TxId, Revision), {}, Cookie);
    }

private:
    i64 Revision;
    uint64_t Cookie;
};

} // anonymous namespace

NActors::IActor* CreateRevisionIncActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, uint64_t cookie) {
    return new TRevisionIncActor(logComponent, std::move(sessionId), std::move(path), txControl, cookie);
}

} // namespace NYdb::NEtcd
