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
    TKVRangeActor(ui64 logComponent, TString&& sessionId, TString path, TTxControl txControl, TString&& txId, ui64 cookie, i64 revision, TRangeRequest&& request)
        : TQueryBase(logComponent, std::move(sessionId), path, path, txControl, std::move(txId), cookie, revision)
        , Request(request) {
    }

    void OnRunQuery() override {
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
            DECLARE $limit AS Int64;

            SELECT kv.*, COUNT(*) OVER() AS count
                FROM kv
                WHERE key BETWEEN $key AND $range_end
                    AND create_revision <= $revision AND (delete_revision IS NULL OR $revision <= delete_revision)
                    AND $min_create_revision <= create_revision
                    AND create_revision <= $max_create_revision
                    AND $min_mod_revision <= mod_revision
                    AND mod_revision <= $max_mod_revision)"
        );

        if (Request.sort_order != TRangeRequest::ESortOrder::NONE) {
            TString order = [&]() {
                switch (Request.sort_order) {
                    case TRangeRequest::ESortOrder::ASCEND:
                        return "ASC";
                    case TRangeRequest::ESortOrder::DESCEND:
                        return "DESC";
                    default:
                        throw std::runtime_error("Unknwon sort order");
                }
            }();

            TString target = [&]() {
                switch (Request.sort_target) {
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

        if (Request.limit > 0) {
            query << R"(
                LIMIT $limit)";
        }

        query << ";";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$revision")
                .Int64(Request.revision == 0 ? Revision : Request.revision)
                .Build()
            .AddParam("$key")
                .String(Request.key)
                .Build()
            .AddParam("$range_end")
                .String(Request.range_end)
                .Build()
            .AddParam("$min_create_revision")
                .Int64(Request.min_create_revision == 0 ? 0 : Request.min_create_revision)
                .Build()
            .AddParam("$max_create_revision")
                .Int64(Request.max_create_revision == 0 ? std::numeric_limits<i64>::max() : Request.max_create_revision)
                .Build()
            .AddParam("$min_mod_revision")
                .Int64(Request.min_mod_revision == 0 ? 0 : Request.min_mod_revision)
                .Build()
            .AddParam("$max_mod_revision")
                .Int64(Request.max_mod_revision == 0 ? std::numeric_limits<i64>::max() : Request.max_mod_revision)
                .Build()
            .AddParam("$limit")
                .Int64(Request.limit + 1) // to fill TRangeResponse::more field
                .Build();

        RunDataQuery(query, &params, TxControl);
    }

    void OnQueryResult() override {
        Y_ABORT_UNLESS(ResultSets.size() == 1, "Unexpected database response");

        NYdb::TResultSetParser parser(ResultSets[0]);

        Response.Count = std::min(parser.RowsCount(), Request.limit);

        Response.More = parser.RowsCount() > Request.limit;

        Response.KVs.reserve(Response.Count);
        while (Response.KVs.size() < Response.Count) {
            parser.TryNextRow();

            TKeyValue kv{
                .key = std::move(parser.ColumnParser("key").GetString()),
                .mod_revision = parser.ColumnParser("mod_revision").GetInt64(),
                .create_revision = *parser.ColumnParser("create_revision").GetOptionalInt64(),
                .version = *parser.ColumnParser("version").GetOptionalInt64(),
                .value = std::move(*parser.ColumnParser("value").GetOptionalString()),
            };
            Response.KVs.emplace_back(std::move(kv));
        }

        DeleteSession = TxControl.Commit;

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvEtcdKV::TEvRangeResponse(status, std::move(issues), SessionId, TxId, std::move(Response)), {}, Cookie);
    }

private:
    TRangeRequest Request;
    TRangeResponse Response;
};

} // anonymous namespace

NActors::IActor* CreateKVQueryActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, ui64 cookie, i64 revision, TRangeRequest request) {
    return new TKVRangeActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), cookie, revision, std::move(request));
}

} // namespace NYdb::NEtcd
