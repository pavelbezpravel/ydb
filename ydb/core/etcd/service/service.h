#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NEtcd {

inline NActors::TActorId MakeEtcdServiceId(ui32 node = 0) {
    return NActors::TActorId(node, TStringBuf("etcd"));
}

NActors::IActor* CreateEtcdService();

} // namespace NYdb::NEtcd
