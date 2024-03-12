#include "kv_txn_compare.h"
#include "events.h"

#include <utility>
#include <ydb/core/base/path.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

TKvTxnCompareActor::TKvTxnCompareActor(ui64 logComponent, TString sessionId, TString path, TVector<TTxnCompareRequest> txnCompareRequests)
    : NKikimr::TQueryBase(logComponent, sessionId, NKikimr::JoinPath({path, "kv"}))
    , Path(std::move(path))
    , TxnCompareRequests(std::move(txnCompareRequests))
{}

void TKvTxnCompareActor::OnRunQuery() {
    auto query = Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        DECLARE $target AS List<Struct<
            key: String,
            op: String,
            target_create_revision: Int64,
            target_mod_revision: Int64,
            target_version: Int64,
            target_value: String,
        >>;

        $compare_type = Enum<Equal, Greater, Less, NotEqual>;
        $compare = ($op, $lhs, $rhs) -> {
            RETURN CASE Enum($op, $compare_type)
                WHEN Enum("Equal",    $compare_type) THEN $lhs == $rhs
                WHEN Enum("Greater",  $compare_type) THEN $lhs >  $rhs
                WHEN Enum("Less",     $compare_type) THEN $lhs <  $rhs
                WHEN Enum("NotEqual", $compare_type) THEN $lhs != $rhs
                ELSE false
            END
        };

        SELECT
                BOOL_AND(COALESCE(CASE
                    WHEN target_create_revision IS NOT NULL THEN $compare(op, create_revision, target_create_revision)
                    WHEN target_mod_revision    IS NOT NULL THEN $compare(op, mod_revision,    target_mod_revision)
                    WHEN target_version         IS NOT NULL THEN $compare(op, version,         target_version)
                    WHEN target_value           IS NOT NULL THEN $compare(op, value,           target_value)
                    ELSE false
                END, false)) AS result,
            FROM AS_TABLE($target) AS target_table
            LEFT JOIN kv           AS source_table USING(key);
    )", Path.c_str());

    // TODO(apozdniakov): validate params

    NYdb::TParamsBuilder params;

    auto& targetParam = params.AddParam("$target");
    targetParam.BeginList();
    for (const auto& TxnCompareRequest : TxnCompareRequests) {
        targetParam.AddListItem();
        auto& structBuilder = targetParam.BeginStruct();
        structBuilder
            .AddMember("key")
                .String(TxnCompareRequest.key)
            .AddMember("op")
                .String([&]() {
                    switch (TxnCompareRequest.result) {
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
                }());
        
        int counter = 0;
        if (TxnCompareRequest.target_create_revision) {
            ++counter;
            structBuilder
                .AddMember("target_create_revision")
                    .OptionalInt64(*TxnCompareRequest.target_create_revision);
        }
        if (TxnCompareRequest.target_mod_revision) {
            ++counter;
            structBuilder
                .AddMember("target_mod_revision")
                    .OptionalInt64(*TxnCompareRequest.target_mod_revision);
        }
        if (TxnCompareRequest.target_version) {
            ++counter;
            structBuilder
                .AddMember("target_version")
                    .OptionalInt64(*TxnCompareRequest.target_version);
        }
        if (TxnCompareRequest.target_value) {
            ++counter;
            structBuilder
                .AddMember("target_value")
                    .OptionalString(*TxnCompareRequest.target_value);
        }
        if (counter != 1) {
            throw std::runtime_error("Expected exactly 1 target field");
        }
        structBuilder.EndStruct();
    }
    targetParam.EndList();
    targetParam.Build();

    params.Build();

    RunDataQuery(query, &params);
}

void TKvTxnCompareActor::OnQueryResult() {
    if (ResultSets.size() != 1) {
        Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
        return;
    }

    NYdb::TResultSetParser parser(ResultSets[0]);

    if (parser.RowsCount() != 1) {
        Finish(Ydb::StatusIds::INTERNAL_ERROR, "Expected 1 row in database response");
        return;
    }

    parser.TryNextRow();

    TxnCompareResponse.succeeded = parser.ColumnParser("result").GetBool();
    
    Finish();
}

void TKvTxnCompareActor::OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
    Send(Owner, new TEvPrivate::TEvTxnCompareResponse(status, std::move(issues), std::move(TxnCompareResponse)));
}

} // namespace NYdb::NEtcd
