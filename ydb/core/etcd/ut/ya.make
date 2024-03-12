UNITTEST_FOR(ydb/core/etcd)

PEERDIR(
    ydb/core/testlib
)

SRCS(
    kv_delete_ut.cpp
    kv_put_ut.cpp
    kv_range_ut.cpp
    kv_txn_compare_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
