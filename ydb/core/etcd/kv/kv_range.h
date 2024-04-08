#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/query_actor/query_actor.h>

namespace NYdb::NEtcd {

struct TRangeRequest;

NActors::IActor* CreateKVRangeActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TRangeRequest rangeRequest);

} // namespace NYdb::NEtcd
