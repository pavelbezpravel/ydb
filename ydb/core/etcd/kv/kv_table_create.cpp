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
    TKVTableCreateActor(ui64 logComponent, TString&& sessionId, TString&& path, uint64_t cookie)
        : LogComponent(logComponent)
        , SessionId(std::move(sessionId))
        , Path(std::move(path))
        , Cookie(cookie) {
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TKVTableCreateActor>::Registered(sys, owner);
        Owner = owner;
    }

    void Bootstrap() {
        Become(&TKVTableCreateActor::CreateTableStateFunc);

        CreateTable();
    }

private:
    STRICT_STFUNC(CreateTableStateFunc, hFunc(NKikimr::TEvTableCreator::TEvCreateTableResponse, Handle))

    void Handle(NKikimr::TEvTableCreator::TEvCreateTableResponse::TPtr& ev) {
        Y_UNUSED(ev);
        Send(Owner, new NYdb::NEtcd::TEvEtcdKV::TEvCreateTableResponse(), {}, Cookie);
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
    ui64 LogComponent;
    TString SessionId;
    NActors::TActorId Owner;

    TString Path;
    uint64_t Cookie;
};

} // anonymous namespace

NActors::IActor* CreateKVTableCreateActor(ui64 logComponent, TString sessionId, TString path, uint64_t cookie) {
    return new TKVTableCreateActor(logComponent, std::move(sessionId), std::move(path), cookie);
}

} // namespace NYdb::NEtcd
