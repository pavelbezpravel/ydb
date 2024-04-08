#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NEtcd {

struct TDeleteRangeRequest;
struct TPutRequest;
struct TRangeRequest;
struct TTxnRequest;

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, TDeleteRangeRequest deleteRequest);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, TPutRequest putRequest);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, TRangeRequest rangeRequest);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, TTxnRequest txnRequest);

} // namespace NYdb::NEtcd
