#include "kv_range.h"

#include "events.h"
#include "proto.h"

#include <utility>

#include <ydb/core/base/path.h>
#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TKVRangeActor : public TQueryBase {
public:
    TKVRangeActor(ui64 logComponent, TString&& sessionId, TString path, TTxControl txControl, uint64_t cookie, TRangeRequest&& request)
        : TQueryBase(logComponent, std::move(sessionId), NKikimr::JoinPath({path, "kv"}), std::move(path), txControl)
        , Cookie(cookie)
        , Request(request) {
    }

    void OnRunQuery() override {
        TStringBuilder query;
        query << Sprintf(R"(
            PRAGMA TablePathPrefix("%s");

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
                WHERE key BETWEEN $key AND $range_end)",
            Path.c_str()
        );

        if (Request.revision > 0) {
            query << R"(
                    AND create_revision <= $revision AND (delete_revision IS NULL OR $revision <= delete_revision))";
        }

        if (Request.min_create_revision > 0) {
            query << R"(
                    AND $min_create_revision <= create_revision)";
        }

        if (Request.max_create_revision > 0) {
            query << R"(
                    AND create_revision <= $max_create_revision)";
        }

        if (Request.min_mod_revision > 0) {
            query << R"(
                    AND $min_mod_revision <= mod_revision)";
        }

        if (Request.max_mod_revision > 0) {
            query << R"(
                    AND mod_revision <= $max_mod_revision)";
        }

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
                .Int64(Request.revision)
                .Build()
            .AddParam("$key")
                .String(Request.key)
                .Build()
            .AddParam("$range_end")
                .String(Request.range_end)
                .Build()
            .AddParam("$min_create_revision")
                .Int64(Request.min_create_revision)
                .Build()
            .AddParam("$max_create_revision")
                .Int64(Request.max_create_revision)
                .Build()
            .AddParam("$min_mod_revision")
                .Int64(Request.min_mod_revision)
                .Build()
            .AddParam("$max_mod_revision")
                .Int64(Request.max_mod_revision)
                .Build()
            .AddParam("$limit")
                .Int64(Request.limit + 1) // to fill TRangeResponse::more field
                .Build()
            .Build();

        RunDataQuery(query, &params, TxControl);
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser parser(ResultSets[0]);

        Response.Count = std::min(parser.RowsCount(), Request.limit);

        Response.More = parser.RowsCount() > Request.limit;

        Response.KVs.reserve(Response.Count);
        while (Response.KVs.size() < Response.Count) {
            parser.TryNextRow();

            TKeyValue kv{
                .key = parser.ColumnParser("key").GetString(),
                .create_revision = parser.ColumnParser("create_revision").GetInt64(),
                .mod_revision = parser.ColumnParser("mod_revision").GetInt64(),
                .version = parser.ColumnParser("version").GetInt64(),
                .value = parser.ColumnParser("value").GetString(),
            };
            Response.KVs.emplace_back(std::move(kv));
        }

        if (TxControl.Commit) {
            CommitTransaction();
            return;
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvEtcdKV::TEvRangeResponse(status, std::move(issues), TxId, std::move(Response)), {}, Cookie);
    }

private:
    uint64_t Cookie;
    TRangeRequest Request;
    TRangeResponse Response;
};

} // anonymous namespace

NActors::IActor* CreateKVRangeActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, uint64_t cookie, TRangeRequest request) {
    return new TKVRangeActor(logComponent, std::move(sessionId), std::move(path), txControl, cookie, std::move(request));
}

} // namespace NYdb::NEtcd
