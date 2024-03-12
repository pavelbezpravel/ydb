#pragma once

#include "proto.h"
#include <ydb/library/query_actor/query_actor.h>

namespace NYdb::NEtcd {

class TKvDeleteActor : public NKikimr::TQueryBase {
public:
    TKvDeleteActor(ui64 logComponent, TString sessionId, TString path, i64 currentRevision, TDeleteRequest deleteRequest);

    void OnRunQuery() override;

    void OnQueryResult() override;

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override;

private:
    TString Path;
    i64 CurrentRevision;
    TDeleteRequest DeleteRequest;
    TDeleteResponse DeleteResponse;
};

} // namespace NYdb::NEtcd
