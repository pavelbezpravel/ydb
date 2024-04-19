#pragma once

#include <ydb/library/query_actor/query_actor.h>

namespace NYdb::NEtcd {

class TQueryBase : public NKikimr::TQueryBase {
public:
    TQueryBase(ui64 logComponent, TString&& sessionId, TString&& database, TString&& path, TTxControl txControl, TString&& txId = {})
        : NKikimr::TQueryBase(logComponent, sessionId, database)
        , Path(path)
        , TxControl(txControl) {
            TxId = std::move(txId);
    }

    static inline std::pair<TTxControl, TTxControl> Split(TTxControl txControl) {
        if (txControl == TTxControl::BeginTx()) {
            return {TTxControl::BeginTx(), TTxControl::ContinueTx()};
        } else if (txControl == TTxControl::BeginAndCommitTx()) {
            return {TTxControl::BeginTx(), TTxControl::ContinueAndCommitTx()};
        } else if (txControl == TTxControl::ContinueTx()) {
            return {TTxControl::ContinueTx(), TTxControl::ContinueTx()};
        } else if (txControl == TTxControl::ContinueAndCommitTx()) {
            return {TTxControl::ContinueTx(), TTxControl::ContinueAndCommitTx()};
        } else {
            Y_ABORT();
        }
    }

protected:
    TString Path;
    TTxControl TxControl;
};

} // namespace NYdb::NEtcd
