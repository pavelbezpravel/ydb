#include "kv_range.h"

#include "events.h"
#include "proto.h"

#include <utility>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TKVRangeActor : public TQueryBase {
public:
    TKVRangeActor(ui64 logComponent, TString&& sessionId, TString path, TTxControl txControl, TString&& txId, i64 revision, i64 compactRevision, TRangeRequest&& request)
        : TQueryBase(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, compactRevision)
        , Request(request) {
        LOG_D("[TKVRangeActor] TKVRangeActor::TKVRangeActor(); TxId: \"" << TxId << "\" SessionId: \"" << SessionId << "\" TxControl: \"" << TxControl.Begin << "\" \"" << TxControl.Commit << "\" \"" << TxControl.Continue << "\" Request: " << Request);
    }

    void OnRunQuery() override {
        if (Request.Revision != 0 && (CompactRevision > Request.Revision || Request.Revision > Revision)) {
            Finish(Ydb::StatusIds::PRECONDITION_FAILED, NYql::TIssues{});
            return;
        }

        auto [compareCond, useRangeEnd] = Compare(Request.Key, Request.RangeEnd);

        TStringBuilder query;
        query << Sprintf(R"(
            PRAGMA TablePathPrefix("/Root/.etcd");

            DECLARE $key AS String;)");
        if (useRangeEnd) {
            query << R"(
            DECLARE $range_end AS String;)";
        }
        if (Request.Revision > 0) {
            query << R"(
            DECLARE $revision AS Int64;)";
        }
        if (Request.Limit > 0) {
            query << R"(
            DECLARE $limit AS Uint64;)";
        }
        query << R"(
            )";
        if (Request.Revision > 0) {
            query << Sprintf(R"(
            SELECT %s
                FROM kv_past
                WHERE %s
                    AND mod_revision <= $revision AND (delete_revision IS NULL OR $revision < delete_revision))",
                Request.CountOnly ? "COUNT(*) AS count" : Request.Limit > 0 ? "COUNT(*) OVER() AS count, kv_past.*" : "*",
                compareCond.data()
            );
            if (Request.Limit > 0) {
                query << R"(
                LIMIT $limit)";
            }
            query << ";";
            query << Sprintf(R"(
            SELECT %s
                FROM kv
                WHERE %s
                    AND mod_revision <= $revision)",
                Request.CountOnly ? "COUNT(*) AS count" : Request.Limit > 0 ? "COUNT(*) OVER() AS count, kv.*" : "*",
                compareCond.data()
            );
            if (Request.Limit > 0) {
                query << R"(
                LIMIT $limit)";
            }
            query << ";";
        } else {
            query << Sprintf(R"(
            SELECT %s
                FROM kv
                WHERE %s)",
                Request.CountOnly ? "COUNT(*) AS count" : Request.Limit > 0 ? "COUNT(*) OVER() AS count, kv.*" : "*",
                compareCond.data()
            );
            if (Request.Limit > 0) {
                query << R"(
                LIMIT $limit)";
            }
            query << ";";
        }

        NYdb::TParamsBuilder params;
        params
            .AddParam("$key")
                .String(Request.Key)
                .Build();
        if (useRangeEnd) {
            params
                .AddParam("$range_end")
                    .String(Request.RangeEnd)
                    .Build();
        }
        if (Request.Revision > 0) {
            params
                .AddParam("$revision")
                    .Int64(Request.Revision)
                    .Build();
        }
        if (Request.Limit > 0) {
            params
                .AddParam("$limit")
                    .Uint64(Request.Limit)
                    .Build();
        }

        RunDataQuery(query, &params, TxControl);
    }

    void OnQueryResult() override {
        Response.Revision = Revision;

        Y_ABORT_UNLESS(ResultSets.size() == 1 + (Request.Revision > 0), "Unexpected database response");

        if (Request.CountOnly) {
            for (const auto& resultSet : ResultSets) {
                NYdb::TResultSetParser parser(resultSet);

                Y_ABORT_UNLESS(parser.RowsCount() == 1, "Expected 1 row in database response");

                parser.TryNextRow();

                Response.Count += parser.ColumnParser("count").GetUint64();
            }
        } else if (Request.Limit > 0) {
            Response.KVs.reserve(Request.Limit);

            for (const auto& resultSet : ResultSets) {
                NYdb::TResultSetParser parser(resultSet);
                
                auto responseCount = 0;
                while (parser.TryNextRow() && Response.KVs.size() < Request.Limit) {
                    responseCount = parser.ColumnParser("count").GetUint64();

                    TKeyValue kv{
                        .Key = std::move(*parser.ColumnParser("key").GetOptionalString()),
                        .ModRevision = *parser.ColumnParser("mod_revision").GetOptionalInt64(),
                        .CreateRevision = *parser.ColumnParser("create_revision").GetOptionalInt64(),
                        .Version = *parser.ColumnParser("version").GetOptionalInt64(),
                        .Value = Request.KeysOnly ? "" : std::move(*parser.ColumnParser("value").GetOptionalString()),
                    };
                    Response.KVs.emplace_back(std::move(kv));
                }
                Response.Count += responseCount;
            }
            Response.More = Response.KVs.size() < Response.Count;
        } else {
            for (const auto& resultSet : ResultSets) {
                NYdb::TResultSetParser parser(resultSet);
                
                Response.KVs.reserve(Response.KVs.size() + parser.RowsCount());
                
                while (parser.TryNextRow()) {
                    TKeyValue kv{
                        .Key = std::move(*parser.ColumnParser("key").GetOptionalString()),
                        .ModRevision = *parser.ColumnParser("mod_revision").GetOptionalInt64(),
                        .CreateRevision = *parser.ColumnParser("create_revision").GetOptionalInt64(),
                        .Version = *parser.ColumnParser("version").GetOptionalInt64(),
                        .Value = Request.KeysOnly ? "" : std::move(*parser.ColumnParser("value").GetOptionalString()),
                    };
                    Response.KVs.emplace_back(std::move(kv));
                }
                Response.Count += parser.RowsCount();
            }
        }
        std::ranges::sort(Response.KVs, std::less{}, &TKeyValue::Key);
        if (Request.SortOrder != TRangeRequest::ESortOrder::NONE) {
            auto apply_sort = [](TVector<TKeyValue>& r, auto comp, auto proj) {
                return std::ranges::stable_sort(r, comp, proj);
            };
            auto apply_proj = [&](TVector<TKeyValue>& r, auto comp) {
                auto impl = [&](auto proj) { return apply_sort(r, comp, proj); };
                switch (Request.SortTarget) {
                    case TRangeRequest::ESortTarget::KEY:
                        return impl(&TKeyValue::Key);
                    case TRangeRequest::ESortTarget::CREATE:
                        return impl(&TKeyValue::CreateRevision);
                    case TRangeRequest::ESortTarget::MOD:
                        return impl(&TKeyValue::ModRevision);
                    case TRangeRequest::ESortTarget::VERSION:
                        return impl(&TKeyValue::Version);
                    case TRangeRequest::ESortTarget::VALUE:
                        return impl(&TKeyValue::Value);
                    default:
                        throw std::runtime_error("Unknown sort target");
                }
            };
            auto apply_comp = [&](TVector<TKeyValue>& r) {
                auto impl = [&](auto comp) { return apply_proj(r, comp); };
                switch (Request.SortOrder) {
                    case TRangeRequest::ESortOrder::ASCEND:
                        return impl(std::less{});
                    case TRangeRequest::ESortOrder::DESCEND:
                        return impl(std::greater{});
                    default:
                        throw std::runtime_error("Unknown sort order");
                }
            };
            apply_comp(Response.KVs);
        }

        DeleteSession = false;

        Y_ABORT_UNLESS(!Response.IsWrite());

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        LOG_D("[TKVRangeActor] TKVRangeActor::OnFinish(); Response: " << Response);
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
        Send(Owner, new TEvEtcdKV::TEvRangeResponse(status, std::move(issues), SessionId, TxId, std::move(Response)));
    }

private:
    TRangeRequest Request;
    TRangeResponse Response{};
};

} // anonymous namespace

NActors::IActor* CreateKVQueryActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, i64 revision, i64 compactRevision, TRangeRequest request) {
    return new TKVRangeActor(logComponent, std::move(sessionId), std::move(path), txControl, std::move(txId), revision, compactRevision, std::move(request));
}

} // namespace NYdb::NEtcd
