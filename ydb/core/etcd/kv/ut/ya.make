UNITTEST_FOR(ydb/core/etcd/kv)

PEERDIR(
    ydb/core/testlib
    ydb/library/yql/sql/pg_dummy
)

SRCS(
    kv_put_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
