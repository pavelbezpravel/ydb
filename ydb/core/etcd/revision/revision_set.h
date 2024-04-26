#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/query_actor/query_actor.h>

namespace NYdb::NEtcd {

NActors::IActor* CreateRevisionSetActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, ui64 cookie, i64 revision, i64 compactRevision);

} // namespace NYdb::NEtcd
