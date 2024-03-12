#pragma once

#include "proto.h"
#include <ydb/library/query_actor/query_actor.h>

namespace NYdb::NEtcd {

class TKvRangeActor : public NKikimr::TQueryBase {
public:
    TKvRangeActor(ui64 logComponent, TString sessionId, TString path, TRangeRequest rangeRequest);

    void OnRunQuery() override;

    void OnQueryResult() override;

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override;

private:
    TString Path;
    TRangeRequest RangeRequest;
    TRangeResponse RangeResponse;
};

} // namespace NYdb::NEtcd
