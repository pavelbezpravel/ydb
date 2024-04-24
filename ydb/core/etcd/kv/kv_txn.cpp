#include "kv_txn.h"

#include "events.h"
#include "proto.h"

#include "kv_delete.h"
#include "kv_put.h"
#include "kv_range.h"


#include <utility>

#include <ydb/core/base/path.h>
#include <ydb/core/etcd/base/query_base.h>

namespace NYdb::NEtcd {

namespace {

class TKVTxnActor : public TQueryBase {
public:
    TKVTxnActor(ui64 logComponent, TString&& sessionId, TString&& path, TTxControl txControl, TString&& txId, ui64 cookie, i64 revision, TTxnRequest&& request, bool isFirstRequest)
        : TQueryBase(logComponent, std::move(sessionId), path, path, txControl, std::move(txId), cookie, revision)
        , RequestIndex(-1)
        , Request(request)
        , IsFirstRequest(isFirstRequest) {
            LOG_E("[TKVTxnActor] TKVTxnActor::TKVTxnActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\" Request: " << Request);
    }

    void OnRunQuery() override {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

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
        )");

        NYdb::TParamsBuilder params;

        auto& targetParam = params.AddParam("$target");
        targetParam.BeginList();
        for (const auto& TxnCmpRequest : Request.Compare) {
            targetParam.AddListItem();
            auto& structBuilder = targetParam.BeginStruct();
            structBuilder
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
                    }());

            int counter = 0;
            if (TxnCmpRequest.TargetCreateRevision) {
                ++counter;
                structBuilder
                    .AddMember("target_create_revision")
                        .OptionalInt64(*TxnCmpRequest.TargetCreateRevision);
            }
            if (TxnCmpRequest.TargetModRevision) {
                ++counter;
                structBuilder
                    .AddMember("target_mod_revision")
                        .OptionalInt64(*TxnCmpRequest.TargetModRevision);
            }
            if (TxnCmpRequest.TargetVersion) {
                ++counter;
                structBuilder
                    .AddMember("target_version")
                        .OptionalInt64(*TxnCmpRequest.TargetVersion);
            }
            if (TxnCmpRequest.TargetValue) {
                ++counter;
                structBuilder
                    .AddMember("target_value")
                        .OptionalString(*TxnCmpRequest.TargetValue);
            }
            if (counter != 1) {
                throw std::runtime_error("Expected exactly 1 target field");
            }
            structBuilder.EndStruct();
        }
        targetParam.EndList();
        targetParam.Build();

        RunDataQuery(query, &params, TxControl);
    }

    void OnCompareQueryResult() {
        Response.Revision = Revision;

        Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

        NYdb::TResultSetParser parser(ResultSets[0]);

        Y_ABORT_UNLESS(parser.RowsCount() == 1, "Expected 1 row in database response");

        parser.TryNextRow();

        Response.Succeeded = parser.ColumnParser("result").GetBool();
        const TVector<TRequestOp>& Requests = Request.Requests[Response.Succeeded];
        Response.Responses.reserve(Requests.size());

        // TODO [pavelbezpravel]: check delete session && IsFirstRequest flags

        if (TxControl.Commit && Requests.empty()) {
            CommitTransaction();
            return;
        }

        RunQuery();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        LOG_E("[TKVTxnActor] TKVTxnActor::TKVTxnActor(); Response: " << Response);
        Send(Owner, new TEvEtcdKV::TEvTxnResponse(status, std::move(issues), SessionId, TxId, std::move(Response)), {}, Cookie);
    }

private:
    STRICT_STFUNC(KVDeleteRangeStateFunc, hFunc(TEvEtcdKV::TEvDeleteRangeResponse, Handle))
    void Handle(TEvEtcdKV::TEvDeleteRangeResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
        }

        Response.Responses.emplace_back(std::make_shared<TDeleteRangeResponse>(std::move(ev->Get()->Response)));

        RunQuery();
    }

    STRICT_STFUNC(KVPutStateFunc, hFunc(TEvEtcdKV::TEvPutResponse, Handle))
    void Handle(TEvEtcdKV::TEvPutResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
        }

        Response.Responses.emplace_back(std::make_shared<TPutResponse>(std::move(ev->Get()->Response)));

        RunQuery();
    }

    STRICT_STFUNC(KVRangeStateFunc, hFunc(TEvEtcdKV::TEvRangeResponse, Handle))
    void Handle(TEvEtcdKV::TEvRangeResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
        }

        Response.Responses.emplace_back(std::make_shared<TRangeResponse>(std::move(ev->Get()->Response)));

        RunQuery();
    }

    STRICT_STFUNC(KVTxnStateFunc, hFunc(TEvEtcdKV::TEvTxnResponse, Handle))
    void Handle(TEvEtcdKV::TEvTxnResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
        }

        Response.Responses.emplace_back(std::make_shared<TTxnResponse>(std::move(ev->Get()->Response)));

        RunQuery();
    }

    void RunQuery() {
        const TVector<TRequestOp>& Requests = Request.Requests[Response.Succeeded];

        if (++RequestIndex == Requests.size()) {
            Finish();
        }
        auto currTxControl = [&]() {
            if (RequestIndex + 1 == Requests.size()) {
                return TxControl;
            } else {
                auto [currTxControl, nextTxControl] = Split(TxControl);
                TxControl = nextTxControl;
                return currTxControl;
            }
        }();
        std::visit([&](const auto& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::shared_ptr<TDeleteRangeRequest>>) {
                Become(&TKVTxnActor::KVDeleteRangeStateFunc);
            } else if constexpr (std::is_same_v<T, std::shared_ptr<TPutRequest>>) {
                Become(&TKVTxnActor::KVPutStateFunc);
            } else if constexpr (std::is_same_v<T, std::shared_ptr<TRangeRequest>>) {
                Become(&TKVTxnActor::KVRangeStateFunc);
            } else if constexpr (std::is_same_v<T, std::shared_ptr<TTxnRequest>>) {
                Become(&TKVTxnActor::KVTxnStateFunc);
            } else {
                static_assert(sizeof(T) == 0);
            }
            Register(CreateKVQueryActor(LogComponent, SessionId, Path, currTxControl, TxId, Cookie, Revision, *arg, std::exchange(IsFirstRequest, false)));
        }, Requests[RequestIndex]);
    }

private:
    size_t RequestIndex;
    TTxnRequest Request;
    TTxnResponse Response;
    bool IsFirstRequest;
};

} // anonymous namespace

NActors::IActor* CreateKVQueryActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, ui64 cookie, i64 revision, TTxnRequest request, bool isFirstRequest) {
    return new TKVTxnActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), cookie, revision, std::move(request), isFirstRequest);
}

} // namespace NYdb::NEtcd
