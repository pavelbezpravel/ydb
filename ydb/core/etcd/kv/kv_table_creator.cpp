#include "kv_table_creator.h"

#include "events.h"

#include <utility>

#include <ydb/core/base/path.h>

#include <ydb/core/etcd/base/query_base.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/table_creator/table_creator.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NYdb::NEtcd {

namespace {

class TKVTableCreatorActor : public TQueryBase {
public:
    TKVTableCreatorActor(ui64 logComponent, TString sessionId, TString path, uint64_t cookie)
        : TQueryBase(logComponent, std::move(sessionId), NKikimr::JoinPath({path, "kv"}), std::move(path), TTxControl::BeginAndCommitTx())
        , Cookie(cookie) {
    }

    void Bootstrap() {
        Become(&TKVTableCreatorActor::CreateTableStateFunc);
        CreateTable();
    }

    void OnRunQuery() override {
        // TODO [pavelbezpravel]: ...
        RunDataQuery({}, {}, {});
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Y_UNUSED(status);
        Y_UNUSED(issues);
        Send(Owner, new TEvEtcdKV::TEvCreateTableResponse(), {}, Cookie);
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
                {Path, "kv"},
                {
                    Col("key", NKikimr::NScheme::NTypeIds::String),
                    Col("mod_revision", NKikimr::NScheme::NTypeIds::Int64),
                    Col("create_revision", NKikimr::NScheme::NTypeIds::Int64),
                    Col("version", NKikimr::NScheme::NTypeIds::Int64),
                    Col("delete_revision", NKikimr::NScheme::NTypeIds::Int64),
                    Col("value", NKikimr::NScheme::NTypeIds::String),
                },
                {"key", "mod_revision"},
                static_cast<NKikimrServices::EServiceKikimr>(LogComponent)
            )
        );
    }

private:
    uint64_t Cookie;
};

} // anonymous namespace

NActors::IActor* CreateKVTableCreatorActor(ui64 logComponent, TString sessionId, TString path, uint64_t cookie) {
    return new TKVTableCreatorActor(logComponent, sessionId, std::move(path), cookie);
}

} // namespace NYdb::NEtcd
