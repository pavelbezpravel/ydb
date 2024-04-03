#include "kv_table_creator.h"

#include <utility>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/table_creator/table_creator.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include "events.h"

namespace NYdb::NEtcd {

namespace {

class TKvTableCreatorActor : public NActors::TActorBootstrapped<TKvTableCreatorActor> {
public:
    TKvTableCreatorActor(ui64 logComponent, TString sessionId, TString path)
        : LogComponent(logComponent)
        , SessionId(std::move(sessionId))
        , Path(std::move(path)) {
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TKvTableCreatorActor>::Registered(sys, owner);
        Owner = owner;
    }

    void Bootstrap() {
        Become(&TKvTableCreatorActor::CreateTableStateFunc);
    }

private:
    STRICT_STFUNC(CreateTableStateFunc, hFunc(NKikimr::TEvTableCreator::TEvCreateTableResponse, Handle))

    void Handle(NKikimr::TEvTableCreator::TEvCreateTableResponse::TPtr&) {
        Finish();
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

    void Finish() {
        Send(Owner, new TEvEtcdKv::TEvCreateTableResponse());
        PassAway();
    }

private:
    ui64 LogComponent;
    TString SessionId;
    NActors::TActorId Owner;

    TString Path;
};

} // anonymous namespace

NActors::IActor* CreateKvTableCreatorActor(ui64 logComponent, TString sessionId, TString path) {
    return new TKvTableCreatorActor(logComponent, sessionId, std::move(path));
}

} // namespace NYdb::NEtcd
