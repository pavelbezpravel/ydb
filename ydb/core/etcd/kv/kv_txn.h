#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/query_actor/query_actor.h>

namespace NYdb::NEtcd {

struct TTxnRequest;

NActors::IActor* CreateKVTxnActor(ui64 logComponent, TString sessionId, TString path, NKikimr::TQueryBase::TTxControl txControl, TString txId, i64 revision, uint64_t cookie, TTxnRequest txnRequest);

} // namespace NYdb::NEtcd
