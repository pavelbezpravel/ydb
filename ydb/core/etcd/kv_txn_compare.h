#pragma once

#include "proto.h"
#include <ydb/library/query_actor/query_actor.h>

namespace NYdb::NEtcd {

class TKvTxnCompareActor : public NKikimr::TQueryBase {
public:
    TKvTxnCompareActor(ui64 logComponent, TString sessionId, TString path, TVector<TTxnCompareRequest> txnCompareRequests);

    void OnRunQuery() override;

    void OnQueryResult() override;

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override;

private:
    TString Path;
    TVector<TTxnCompareRequest> TxnCompareRequests;
    TTxnCompareResponse TxnCompareResponse;
};

} // namespace NYdb::NEtcd
