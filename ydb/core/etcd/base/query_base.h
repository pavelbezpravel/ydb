#pragma once

#include <limits>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/query_actor/query_actor.h>

#define LOG_T(stream) LOG_TRACE_S(*NActors::TlsActivationContext, LogComponent, "[ydb] [ETCD_FEATURE]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, LogComponent, "[ydb] [ETCD_FEATURE]: " << stream)
#define LOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, LogComponent, "[ydb] [ETCD_FEATURE]: " << stream)
#define LOG_N(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, LogComponent, "[ydb] [ETCD_FEATURE]: " << stream)
#define LOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, LogComponent, "[ydb] [ETCD_FEATURE]: " << stream)
#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, LogComponent, "[ydb] [ETCD_FEATURE]: " << stream)
#define LOG_C(stream) LOG_CRIT_S(*NActors::TlsActivationContext, LogComponent, "[ydb] [ETCD_FEATURE]: " << stream)

namespace NYdb::NEtcd {

class TQueryBase : public NKikimr::TQueryBase {
public:
    TQueryBase(ui64 logComponent, TString&& sessionId, TString&& database, TTxControl txControl, TString&& txId, i64 revision, i64 compactRevision)
        : NKikimr::TQueryBase(logComponent, sessionId, database)
        , TxControl(txControl)
        , Revision(revision)
        , CompactRevision(compactRevision) {
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

    [[nodiscard]] static inline TString Compare(const TString& key, const TString& rangeEnd) noexcept {
        if (rangeEnd.empty()) {
            return "key == $key";
        } else if (rangeEnd == kEmptyKey) {
            return "key >= $key";
        } else if (rangeEnd == GetPrefix(key)) {
            return "StartsWith(key, $key)";
        } else {
            return "key BETWEEN $key AND $range_end";
        }
    }

protected:
    static constexpr TStringBuf kEmptyKey{"\0", 1};
    static_assert(kEmptyKey.size() == 1);

    TTxControl TxControl;
    i64 Revision;
    i64 CompactRevision;
};

} // namespace NYdb::NEtcd
