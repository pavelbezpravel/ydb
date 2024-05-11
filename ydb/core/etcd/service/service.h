#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimrConfig {
class TQueryServiceConfig;
} // namespace NKikimrConfig

namespace NYdb::NEtcd {

inline NActors::TActorId MakeEtcdServiceId(ui32 node = 0) {
    return NActors::TActorId(node, TStringBuf("etcd"));
}

NActors::IActor* CreateEtcdService(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig);

} // namespace NYdb::NEtcd
