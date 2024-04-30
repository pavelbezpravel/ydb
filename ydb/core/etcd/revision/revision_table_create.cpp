#include "revision_table_create.h"

#include <utility>

#include <ydb/core/etcd/revision/events.h>
#include <ydb/core/etcd/revision/revision_set.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/table_creator/table_creator.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NYdb::NEtcd {

namespace {

class TRevisionTableCreateActor : public NActors::TActorBootstrapped<TRevisionTableCreateActor> {
public:
    TRevisionTableCreateActor(ui64 logComponent, TString&& path)
        : LogComponent(logComponent)
        , Path(std::move(path)) {
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TRevisionTableCreateActor>::Registered(sys, owner);
        Owner = owner;
    }

    void Bootstrap() {
        CreateTable();
    }

private:
    STRICT_STFUNC(CreateTableStateFunc, hFunc(NKikimr::TEvTableCreator::TEvCreateTableResponse, Handle))

    void Handle(NKikimr::TEvTableCreator::TEvCreateTableResponse::TPtr&) {
        InitTable();
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
        Become(&TRevisionTableCreateActor::CreateTableStateFunc);

        Register(
            NKikimr::CreateTableCreator(
                {".etcd", "revision"},
                {
                    Col("id", NKikimr::NScheme::NTypeIds::Bool),
                    Col("revision", NKikimr::NScheme::NTypeIds::Int64),
                },
                {"id"},
                static_cast<NKikimrServices::EServiceKikimr>(LogComponent))
        );
    }

    STRICT_STFUNC(StateFunc, hFunc(TEvEtcdRevision::TEvRevisionResponse, Handle))

    void Handle(TEvEtcdRevision::TEvRevisionResponse::TPtr& ev) {
        Send(Owner, new TEvEtcdRevision::TEvCreateTableResponse(ev->Get()->Status, std::move(ev->Get()->Issues), ev->Get()->SessionId));
        PassAway();
    }

    void InitTable() {
        Become(&TRevisionTableCreateActor::StateFunc);

        Register(CreateRevisionSetActor(LogComponent, {}, Path, NKikimr::TQueryBase::TTxControl::BeginAndCommitTx(), {}, 1, 0));
    }

private:
    ui64 LogComponent;
    TString Path;
    NActors::TActorId Owner;
};

} // anonymous namespace

NActors::IActor* CreateRevisionTableCreateActor(ui64 logComponent, TString path) {
    return new TRevisionTableCreateActor(logComponent, std::move(path));
}

} // namespace NYdb::NEtcd
