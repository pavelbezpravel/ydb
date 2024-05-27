#include "kv_table_create.h"

#include <utility>

#include <ydb/core/etcd/kv/events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/table_creator/table_creator.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NYdb::NEtcd {

namespace {

class TKVTableCreateActor : public NActors::TActorBootstrapped<TKVTableCreateActor> {
public:
    TKVTableCreateActor(ui64 logComponent, TString&& path)
        : LogComponent(logComponent)
        , Path(std::move(path)) {
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TKVTableCreateActor>::Registered(sys, owner);
        Owner = owner;
    }

    void Bootstrap() {
        Become(&TKVTableCreateActor::CreateTableStateFunc);

        CreateKvTable();
        CreateKvPastTable();
    }

private:
    STRICT_STFUNC(CreateTableStateFunc, hFunc(NKikimr::TEvTableCreator::TEvCreateTableResponse, Handle))

    void Handle(NKikimr::TEvTableCreator::TEvCreateTableResponse::TPtr&) {
        Y_ABORT_UNLESS(TablesCreating > 0);
        if (--TablesCreating > 0) {
            return;
        }
        Send(Owner, new TEvEtcdKV::TEvCreateTableResponse());
        PassAway();
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

    void CreateKvTable() {
        ++TablesCreating;
        Register(
            NKikimr::CreateTableCreator(
                {".etcd", "kv"},
                {
                    Col("key", NKikimr::NScheme::NTypeIds::String),
                    Col("mod_revision", NKikimr::NScheme::NTypeIds::Int64),
                    Col("create_revision", NKikimr::NScheme::NTypeIds::Int64),
                    Col("version", NKikimr::NScheme::NTypeIds::Int64),
                    Col("value", NKikimr::NScheme::NTypeIds::String),
                },
                {"key"},
                static_cast<NKikimrServices::EServiceKikimr>(LogComponent)
            )
        );
    }

    void CreateKvPastTable() {
        ++TablesCreating;
        Register(
            NKikimr::CreateTableCreator(
                {".etcd", "kv_past"},
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
    ui64 LogComponent;
    TString Path;
    NActors::TActorId Owner;
    size_t TablesCreating = 0;
};

} // anonymous namespace

NActors::IActor* CreateKVTableCreateActor(ui64 logComponent, TString path) {
    return new TKVTableCreateActor(logComponent, std::move(path));
}

} // namespace NYdb::NEtcd
