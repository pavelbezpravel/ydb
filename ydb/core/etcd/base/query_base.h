#pragma once

#include <limits>

#include <limits>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/query_actor/query_actor.h>

#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_PROXY, "[ydb] [ETCD_FEATURE]: " << stream)

namespace NYdb::NEtcd {

class TQueryBase : public NKikimr::TQueryBase {
public:
    TQueryBase(ui64 logComponent, TString&& sessionId, TString database, TString path, TTxControl txControl, TString&& txId, ui64 cookie, i64 revision)
        : NKikimr::TQueryBase(logComponent, sessionId, database)
        , Path(path)
        , TxControl(txControl)
        , Cookie(cookie)
        , Revision(revision) {
        TxId = std::move(txId);
    }

    [[nodiscard]] static inline std::pair<TTxControl, TTxControl> Split(TTxControl txControl) noexcept {
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

    [[nodiscard]] static inline TString GetPrefix(TString key) noexcept {
        while (!key.empty()) {
            if (static_cast<unsigned char>(key.back()) < std::numeric_limits<unsigned char>::max()) {
                char key_back = key.back();
                ++key_back;
                key.back() = key_back;
                return key;
            }
            key.pop_back();
        }
        return TString{kEmptyKey};
    }

protected:
    static constexpr TStringBuf kEmptyKey{"\0", 1};
    static_assert(kEmptyKey.size() == 1);

    TString Path;
    TTxControl TxControl;

    ui64 Cookie;
    i64 Revision;
};

} // namespace NYdb::NEtcd
