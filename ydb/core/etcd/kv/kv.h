#pragma once

#include "proto.h"
#include <ydb/library/actors/core/actor.h>

namespace NYdb::NEtcd {

NActors::IActor* CreateKvActor(ui64 logComponent, TString sessionId, TString path, TDeleteRequest deleteRequest);

NActors::IActor* CreateKvActor(ui64 logComponent, TString sessionId, TString path, TPutRequest putRequest);

NActors::IActor* CreateKvActor(ui64 logComponent, TString sessionId, TString path, TRangeRequest rangeRequest);

NActors::IActor* CreateKvActor(ui64 logComponent, TString sessionId, TString path, TTxnRequest txnRequest);

} // namespace NYdb::NEtcd
