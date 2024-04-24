#include "kv_range.h"

#include "events.h"
#include "proto.h"

#include <limits>
#include <utility>

#include <ydb/core/base/path.h>
#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TKVRangeActor : public TQueryBase {
public:
    TKVRangeActor(ui64 logComponent, TString&& sessionId, TString path, TTxControl txControl, TString&& txId, ui64 cookie, i64 revision, TRangeRequest&& request, bool isFirstRequest)
        : TQueryBase(logComponent, std::move(sessionId), path, path, txControl, std::move(txId), cookie, revision)
        , CommitTx(std::exchange(TxControl.Commit, false))
        , Request(request)
        , IsFirstRequest(isFirstRequest) {
        LOG_E("[TKVRangeActor] TKVRangeActor::TKVRangeActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\" Request: " << Request << " IsFirstRequest: " << IsFirstRequest);
    }

    void OnRunQuery() override {
        auto compareCond = Compare(Request.Key, Request.RangeEnd);

        TStringBuilder query;
        query << Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

            DECLARE $revision AS Int64;
            DECLARE $key AS String;
            DECLARE $range_end AS String;
            DECLARE $min_create_revision AS Int64;
            DECLARE $max_create_revision AS Int64;
            DECLARE $min_mod_revision AS Int64;
            DECLARE $max_mod_revision AS Int64;
            DECLARE $limit AS Uint64;

            SELECT kv.*, COUNT(*) OVER() AS count
                FROM kv
                WHERE %s
                    AND mod_revision <= $revision AND (delete_revision IS NULL OR $revision < delete_revision)
                    AND $min_create_revision <= create_revision
                    AND create_revision <= $max_create_revision
                    AND $min_mod_revision <= mod_revision
                    AND mod_revision <= $max_mod_revision)",
            compareCond.c_str()
        );

        if (Request.SortOrder != TRangeRequest::ESortOrder::NONE) {
            TString order = [&]() {
                switch (Request.SortOrder) {
                    case TRangeRequest::ESortOrder::ASCEND:
                        return "ASC";
                    case TRangeRequest::ESortOrder::DESCEND:
                        return "DESC";
                    default:
                        throw std::runtime_error("Unknwon sort order");
                }
            }();

            TString target = [&]() {
                switch (Request.SortTarget) {
                    case TRangeRequest::ESortTarget::KEY:
                        return "key";
                    case TRangeRequest::ESortTarget::CREATE:
                        return "create_revision";
                    case TRangeRequest::ESortTarget::MOD:
                        return "mod_revision";
                    case TRangeRequest::ESortTarget::VERSION:
                        return "version";
                    case TRangeRequest::ESortTarget::VALUE:
                        return "value";
                    default:
                        throw std::runtime_error("Unknwon sort target");
                }
            }();

            query << Sprintf(R"(
                    ORDER BY %s %s)", target.c_str(), order.c_str());
        }

        if (Request.Limit > 0) {
            query << R"(
                LIMIT $limit)";
        }

        query << ";";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$revision")
                .Int64(Request.Revision == 0 ? Revision : Request.Revision)
                .Build()
            .AddParam("$key")
                .String(Request.Key)
                .Build()
            .AddParam("$range_end")
                .String(Request.RangeEnd)
                .Build()
            .AddParam("$min_create_revision")
                .Int64(Request.MinCreateRevision == 0 ? 0 : Request.MinCreateRevision)
                .Build()
            .AddParam("$max_create_revision")
                .Int64(Request.MaxCreateRevision == 0 ? std::numeric_limits<i64>::max() : Request.MaxCreateRevision)
                .Build()
            .AddParam("$min_mod_revision")
                .Int64(Request.MinModRevision == 0 ? 0 : Request.MinModRevision)
                .Build()
            .AddParam("$max_mod_revision")
                .Int64(Request.MaxModRevision == 0 ? std::numeric_limits<i64>::max() : Request.MaxModRevision)
                .Build()
            .AddParam("$limit")
                .Uint64(Request.Limit + 1) // to fill TRangeResponse::more field
                .Build();

        RunDataQuery(query, &params, TxControl);
    }

    void OnQueryResult() override {
        Response.Revision = Revision;

        Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

        NYdb::TResultSetParser parser(ResultSets[0]);

        Response.Count = 0;
        auto responseCount = Request.Limit == 0 ? parser.RowsCount() : std::min(parser.RowsCount(), Request.Limit);

        Response.More = parser.RowsCount() > responseCount;

        Response.KVs.reserve(responseCount);
        while (Response.KVs.size() < responseCount) {
            parser.TryNextRow();
            Response.Count= parser.ColumnParser("count").GetUint64();

            TKeyValue kv{
                .Key = std::move(*parser.ColumnParser("key").GetOptionalString()),
                .ModRevision = *parser.ColumnParser("mod_revision").GetOptionalInt64(),
                .CreateRevision = *parser.ColumnParser("create_revision").GetOptionalInt64(),
                .Version = *parser.ColumnParser("version").GetOptionalInt64(),
                .Value = std::move(*parser.ColumnParser("value").GetOptionalString()),
            };
            Response.KVs.emplace_back(std::move(kv));
        }

        DeleteSession = IsFirstRequest || (CommitTx && !Response.IsWrite());

        if (DeleteSession) {
            CommitTransaction();
            return;
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        LOG_E("[TKVRangeActor] TKVRangeActor::OnFinish(); Response: " << Response);
        Send(Owner, new TEvEtcdKV::TEvRangeResponse(status, std::move(issues), SessionId, TxId, std::move(Response)), {}, Cookie);
    }

private:
    bool CommitTx;
    TRangeRequest Request;
    TRangeResponse Response;
    bool IsFirstRequest;
};

} // anonymous namespace

NActors::IActor* CreateKVQueryActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, ui64 cookie, i64 revision, TRangeRequest request, bool isFirstRequest) {
    return new TKVRangeActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), cookie, revision, std::move(request), isFirstRequest);
}

} // namespace NYdb::NEtcd
