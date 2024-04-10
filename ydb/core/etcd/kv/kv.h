#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NEtcd {

struct TDeleteRangeRequest;
struct TPutRequest;
struct TRangeRequest;
struct TTxnRequest;

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, uint64_t cookie, TDeleteRangeRequest deleteRequest);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, uint64_t cookie, TPutRequest putRequest);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, uint64_t cookie, TRangeRequest rangeRequest);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, uint64_t cookie, TTxnRequest txnRequest);

} // namespace NYdb::NEtcd
