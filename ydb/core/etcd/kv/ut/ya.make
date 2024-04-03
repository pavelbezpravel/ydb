UNITTEST_FOR(ydb/core/etcd/kv)

PEERDIR(
    ydb/core/testlib
    ydb/library/yql/sql/pg_dummy
)

SRCS(
    kv_delete_ut.cpp
    kv_put_ut.cpp
    kv_range_ut.cpp
    kv_table_creator_ut.cpp
    kv_txn_ut.cpp
    kv_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
