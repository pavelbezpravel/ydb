#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NEtcd {

NActors::IActor* CreateRevisionTableCreateActor(ui64 logComponent, TString path);

} // namespace NYdb::NEtcd
