#include "kv_txn_compare.h"

#include "events.h"
#include "proto.h"

#include <utility>

#include <ydb/core/base/path.h>
#include <ydb/core/etcd/base/query_base.h>

namespace NYdb::NEtcd {

namespace {

class TKVTxnCompareActor : public TQueryBase {
public:
    TKVTxnCompareActor(ui64 logComponent, TString&& sessionId, TString&& path, TTxControl txControl, TString&& txId, ui64 cookie, i64 revision, TVector<TTxnCompareRequest>&& request, std::array<size_t, 2> requestSizes)
        : TQueryBase(logComponent, std::move(sessionId), path, path, txControl, std::move(txId), cookie, revision)
        , CommitTx(std::exchange(TxControl.Commit, false))
        , RequestSizes(requestSizes)
        , Request(request) {
            LOG_E("[TKVTxnCompareActor] TKVTxnCompareActor::TKVTxnCompareActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\"");
    }

    void OnRunQuery() override {
        if (Request.empty()) {
            Response.Succeeded = true;
            Finish();
            return;
        }

        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

            DECLARE $target AS List<Struct<
                key: String,
                op: String,
                target_create_revision: Optional<Int64>,
                target_mod_revision: Optional<Int64>,
                target_version: Optional<Int64>,
                target_value: Optional<String>,
            >>;

            $compare = ($op, $lhs, $rhs) -> {
                RETURN CASE $op
                    WHEN "Equal"    THEN $lhs == $rhs
                    WHEN "Greater"  THEN $lhs >  $rhs
                    WHEN "Less"     THEN $lhs <  $rhs
                    WHEN "NotEqual" THEN $lhs != $rhs
                    ELSE false
                END
            };

            SELECT
                    BOOL_AND(CASE
                        WHEN target_create_revision IS NOT NULL THEN $compare(op, create_revision, target_create_revision)
                        WHEN target_mod_revision    IS NOT NULL THEN $compare(op, mod_revision,    target_mod_revision)
                        WHEN target_version         IS NOT NULL THEN $compare(op, version,         target_version)
                        WHEN target_value           IS NOT NULL THEN $compare(op, value,           target_value)
                        ELSE false
                    END) AS result,
                FROM AS_TABLE($target) AS target_table
                LEFT JOIN kv           AS source_table USING(key);)"
        );

        NYdb::TParamsBuilder params;

        auto& targetParam = params.AddParam("$target");
        targetParam.BeginList();
        for (const auto& TxnCmpRequest : Request) {
            targetParam.AddListItem()
                .BeginStruct()
                .AddMember("key")
                    .String(TxnCmpRequest.Key)
                .AddMember("op")
                    .String([&]() {
                        switch (TxnCmpRequest.Result) {
                            case TTxnCompareRequest::ECompareResult::EQUAL:
                                return "Equal";
                            case TTxnCompareRequest::ECompareResult::GREATER:
                                return "Greater";
                            case TTxnCompareRequest::ECompareResult::LESS:
                                return "Less";
                            case TTxnCompareRequest::ECompareResult::NOT_EQUAL:
                                return "NotEqual";
                            default:
                                throw std::runtime_error("Unexpected compare type");
                        }
                    }())
                .AddMember("target_create_revision")
                    .OptionalInt64(TxnCmpRequest.TargetCreateRevision)
                .AddMember("target_mod_revision")
                    .OptionalInt64(TxnCmpRequest.TargetModRevision)
                .AddMember("target_version")
                    .OptionalInt64(TxnCmpRequest.TargetVersion)
                .AddMember("target_value")
                    .OptionalString(TxnCmpRequest.TargetValue)
                .EndStruct();
        }
        targetParam.EndList();
        targetParam.Build();

        RunDataQuery(query, &params, TxControl);
    }

    void OnQueryResult() override {
        Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

        NYdb::TResultSetParser parser(ResultSets[0]);

        Y_ABORT_UNLESS(parser.RowsCount() == 1, "Expected 1 row in database response");

        parser.TryNextRow();

        Response.Succeeded = parser.ColumnParser("result").GetOptionalBool().GetOrElse(false);

        DeleteSession = CommitTx && RequestSizes[!Response.Succeeded] == 0;

        if (DeleteSession) {
            CommitTransaction();
            return;
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        LOG_E("[TKVTxnCompareActor] TKVTxnCompareActor::OnFinish(); Response: " << Response);
        Send(Owner, new TEvEtcdKV::TEvTxnCompareResponse(status, std::move(issues), SessionId, TxId, std::move(Response)), {}, Cookie);
    }

private:
    bool CommitTx;
    std::array<size_t, 2> RequestSizes;
    TVector<TTxnCompareRequest> Request;
    TTxnCompareResponse Response;
};

} // anonymous namespace

NActors::IActor* CreateKVTxnCompareActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, ui64 cookie, i64 revision, TVector<TTxnCompareRequest> request, std::array<size_t, 2> requestSizes) {
    return new TKVTxnCompareActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), cookie, revision, std::move(request), requestSizes);
}

} // namespace NYdb::NEtcd
