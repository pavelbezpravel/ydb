#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NEtcd {

struct TCompactionRequest;
struct TDeleteRangeRequest;
struct TPutRequest;
struct TRangeRequest;
struct TTxnRequest;

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TCompactionRequest request, bool isFirstRequest);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TDeleteRangeRequest request, bool isFirstRequest);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TPutRequest request, bool isFirstRequest);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TRangeRequest request, bool isFirstRequest);

NActors::IActor* CreateKVActor(ui64 logComponent, TString sessionId, TString path, ui64 cookie, TTxnRequest request, bool isFirstRequest);

} // namespace NYdb::NEtcd
