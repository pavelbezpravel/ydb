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
    TKVCompactActor(ui64 logComponent, TString&& sessionId, TString&& path, TTxControl txControl, TString&& txId, i64 revision, i64 compactRevision, TCompactionRequest&& request)
        : TQueryBase(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, compactRevision)
        , CommitTx(std::exchange(TxControl.Commit, false))
        , Request(request) {
        TxControl.Commit = false;
        LOG_D("[TKVCompactActor] TKVCompactActor::TKVCompactActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\" Request: " << request);
    }

    void OnRunQuery() override {
        if (Request.Revision != 0 && (CompactRevision > Request.Revision || Request.Revision > Revision)) {
            CommitTransaction();
            return;
        }

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
        LOG_D("[TKVCompactActor] TKVCompactActor::OnFinish(); Owner: " << Owner << ", Response: " << Response << ", Issues: \"" << issues.ToString() << "\" Status: " << status);
        if (Request.Revision != 0 && Request.Revision > Revision) {
            auto errMessage = NYql::TIssue{"etcdserver: mvcc: required revision is a future revision"};
            status = Ydb::StatusIds::PRECONDITION_FAILED;
            issues.Clear();
            issues.AddIssue(errMessage);
        } else if (Request.Revision != 0 && Request.Revision < CompactRevision) {
            auto errMessage = NYql::TIssue{"etcdserver: mvcc: required revision has been compacted"};
            status = Ydb::StatusIds::PRECONDITION_FAILED;
            issues.Clear();
            issues.AddIssue(errMessage);
        }
        Send(Owner, new TEvEtcdKV::TEvCompactionResponse(status, std::move(issues), SessionId, TxId, std::move(Response)));
    }

private:
    bool CommitTx;
    TCompactionRequest Request;
    TCompactionResponse Response;
};

} // anonymous namespace

NActors::IActor* CreateKVQueryActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, i64 revision, i64 compactRevision, TCompactionRequest request) {
    return new TKVCompactActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, compactRevision, std::move(request));
}

} // namespace NYdb::NEtcd
