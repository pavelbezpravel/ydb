#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NEtcd {

NActors::IActor* CreateRevisionTableCreateActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie);

} // namespace NYdb::NEtcd
