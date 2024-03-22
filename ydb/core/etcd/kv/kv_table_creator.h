#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NEtcd {

NActors::IActor* CreateKvTableCreatorActor(ui64 logComponent, TString sessionId, TString path);

} // namespace NYdb::NEtcd
