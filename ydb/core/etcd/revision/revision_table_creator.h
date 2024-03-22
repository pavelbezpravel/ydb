#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NEtcd {

NActors::IActor* CreateRevisionTableCreatorActor(ui64 logComponent, TString sessionId, TString path);

} // namespace NYdb::NEtcd
