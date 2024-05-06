#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/query_actor/query_actor.h>

namespace NYdb::NEtcd {

struct TTxnRequest;

NActors::IActor* CreateKVQueryActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, i64 revision, i64 compactRevision, TTxnRequest request);

} // namespace NYdb::NEtcd
