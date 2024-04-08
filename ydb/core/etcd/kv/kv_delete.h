#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/query_actor/query_actor.h>

namespace NYdb::NEtcd {

struct TDeleteRangeRequest;

NActors::IActor* CreateKVDeleteActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, i64 revision, TDeleteRangeRequest request);

} // namespace NYdb::NEtcd
