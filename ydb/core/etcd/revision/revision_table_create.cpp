#include "revision_table_create.h"

#include <utility>

#include <ydb/core/etcd/revision/events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/table_creator/table_creator.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NYdb::NEtcd {

namespace {

class TRevisionTableCreateActor : public NActors::TActorBootstrapped<TRevisionTableCreateActor> {
public:
    TRevisionTableCreateActor(ui64 logComponent, TString&& sessionId, TString&& path, ui64 cookie)
        : LogComponent(logComponent)
        , SessionId(std::move(sessionId))
        , Path(std::move(path))
        , Cookie(cookie) {
    }

    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override {
        NActors::TActorBootstrapped<TRevisionTableCreateActor>::Registered(sys, owner);
        Owner = owner;
    }

    void Bootstrap() {
        Become(&TRevisionTableCreateActor::CreateTableStateFunc);

        CreateTable();
    }

private:
    STRICT_STFUNC(CreateTableStateFunc, hFunc(NKikimr::TEvTableCreator::TEvCreateTableResponse, Handle))

    void Handle(NKikimr::TEvTableCreator::TEvCreateTableResponse::TPtr& ev) {
        Y_UNUSED(ev);
        Send(Owner, new NYdb::NEtcd::TEvEtcdRevision::TEvCreateTableResponse(), {}, Cookie);
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
                {".etcd", "revision"},
                {
                    Col("id", NKikimr::NScheme::NTypeIds::Bool),
                    Col("revision", NKikimr::NScheme::NTypeIds::Int64),
                },
                {"id"},
                static_cast<NKikimrServices::EServiceKikimr>(LogComponent))
        );
    }

private:
    ui64 LogComponent;
    TString SessionId;
    NActors::TActorId Owner;

    TString Path;
    ui64 Cookie;
};

} // anonymous namespace

NActors::IActor* CreateRevisionTableCreateActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie) {
    return new TRevisionTableCreateActor(logComponent, std::move(sessionId), std::move(path), cookie);
}

} // namespace NYdb::NEtcd
