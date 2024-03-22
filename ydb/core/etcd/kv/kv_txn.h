#pragma once

#include "proto.h"
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/query_actor/query_actor.h>

namespace NYdb::NEtcd {

NActors::IActor* CreateKvTxnActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, i64 revision, TTxnRequest txnRequest);

} // namespace NYdb::NEtcd
