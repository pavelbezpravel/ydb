#include "kv_compact.h"

#include "events.h"
#include "proto.h"

#include <utility>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TKVCompactActor : public TQueryBase {
public:
    TKVCompactActor(ui64 logComponent, TString&& sessionId, TString&& path, TTxControl txControl, TString&& txId, i64 revision, TCompactionRequest&& request)
        : TQueryBase(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision)
        , CommitTx(std::exchange(TxControl.Commit, false))
        , Request(request) {
    }

    void OnRunQuery() override {
        TStringBuilder query;
        query << Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

            DECLARE $revision AS Int64;

            DELETE
                FROM kv
                WHERE delete_revision <= $revision;)"
        );

        NYdb::TParamsBuilder params;
        params
            .AddParam("$revision")
                .Int64(Request.Revision)
                .Build();

        RunDataQuery(query, &params, TxControl);
    }

    void OnQueryResult() override {
        Response.Revision = Revision;

        Y_ABORT_UNLESS(ResultSets.empty(), "Unexpected database response");

        DeleteSession = CommitTx && !Response.IsWrite();

        if (DeleteSession) {
            CommitTransaction();
            return;
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvEtcdKV::TEvCompactionResponse(status, std::move(issues), SessionId, TxId, std::move(Response)));
    }

private:
    bool CommitTx;
    TCompactionRequest Request;
    TCompactionResponse Response;
};

} // anonymous namespace

NActors::IActor* CreateKVQueryActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, i64 revision, TCompactionRequest request) {
    return new TKVCompactActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, std::move(request));
}

} // namespace NYdb::NEtcd
