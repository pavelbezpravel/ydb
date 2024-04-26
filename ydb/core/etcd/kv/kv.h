#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NEtcd {

struct TCompactionRequest;
struct TDeleteRangeRequest;
struct TPutRequest;
struct TRangeRequest;
struct TTxnRequest;

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TCompactionRequest request);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TDeleteRangeRequest request);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TPutRequest request);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TRangeRequest request);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TTxnRequest request);

} // namespace NYdb::NEtcd
