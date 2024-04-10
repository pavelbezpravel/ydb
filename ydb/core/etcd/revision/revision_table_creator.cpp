#include "revision_table_creator.h"

#include "events.h"
#include "query_base.h"

#include <utility>

#include <ydb/core/base/path.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/table_creator/table_creator.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NYdb::NEtcd {

namespace {

class TRevisionTableCreatorActor : public TQueryBase {
public:
    TRevisionTableCreatorActor(ui64 logComponent, TString&& sessionId, TString path, uint64_t cookie)
        : TQueryBase(logComponent, std::move(sessionId), NKikimr::JoinPath({path, "revision"}), std::move(path), TTxControl::BeginAndCommitTx())
        , Cookie(cookie) {
    }

    void Bootstrap() {
        Become(&TRevisionTableCreatorActor::CreateTableStateFunc);
    }

    void OnRunQuery() override {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("%s");

            $initial = AsList(
                AsStruct(id: FALSE, revision: 0),
            );
            UPSERT
                INTO revision (id, revision)
                SELECT *
                    FROM AS_TABLE($initial);
            SELECT revision FROM AS_TABLE($initial);
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
    STRICT_STFUNC(CreateTableStateFunc, hFunc(NKikimr::TEvTableCreator::TEvCreateTableResponse, Handle))

    void Handle(NKikimr::TEvTableCreator::TEvCreateTableResponse::TPtr&) {
        TQueryBase::Bootstrap();
    }

    static NKikimrSchemeOp::TColumnDescription Col(const TString& columnName, const char* columnType) {
        NKikimrSchemeOp::TColumnDescription desc;
        desc.SetName(columnName);
        desc.SetType(columnType);
        return desc;
    }

    static NKikimrSchemeOp::TColumnDescription Col(const TString& columnName, NKikimr::NScheme::TTypeId columnType) {
        return Col(columnName, NKikimr::NScheme::TypeName(columnType));
    }

    void CreateTable() {
        Register(
            NKikimr::CreateTableCreator(
                {Path, "revision"},
                {
                    Col("id", NKikimr::NScheme::NTypeIds::Bool),
                    Col("revision", NKikimr::NScheme::NTypeIds::Int64),
                },
                {"id"},
                static_cast<NKikimrServices::EServiceKikimr>(LogComponent))
        );
    }

private:
    i64 Revision;
    uint64_t Cookie;
};

} // anonymous namespace

NActors::IActor* CreateRevisionTableCreatorActor(ui64 logComponent, TString sessionId, TString path, uint64_t cookie) {
    return new TRevisionTableCreatorActor(logComponent, std::move(sessionId), std::move(path), cookie);
}

} // namespace NYdb::NEtcd
