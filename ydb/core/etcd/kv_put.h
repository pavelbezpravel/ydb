#pragma once

#include "proto.h"
#include <ydb/library/query_actor/query_actor.h>

namespace NYdb::NEtcd {

class TKvPutActor : public NKikimr::TQueryBase {
public:
    TKvPutActor(ui64 logComponent, TString sessionId, TString path, i64 currentRevision, TPutRequest putRequest);

    void OnRunQuery() override;

    void OnQueryResult() override;

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override;

private:
    TString Path;
    i64 CurrentRevision;
    TPutRequest PutRequest;
    TPutResponse PutResponse;
};

} // namespace NYdb::NEtcd
