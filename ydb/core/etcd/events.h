#pragma once

#include <ydb/core/base/events.h>

#include <ydb/public/api/etcd/api/etcdserverpb/rpc.grpc.pb.h>

namespace NKikimr::NEtcd {

struct TEvEtcd {
    enum EEv {
        EvRangeResponse = EventSpaceBegin(NKikimr::TKikimrEvents::ES_ETCD),

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::TKikimrEvents::ES_ETCD),
        "expect EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_ETCD)");

    struct TEvRangeResponse : public TEventLocal<TEvRangeResponse, TEvEtcd::EvRangeResponse> {};

};

} // namespace NKikimr::NEtcd