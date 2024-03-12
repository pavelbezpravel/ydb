LIBRARY()

SRCS(
    kv_delete.cpp
    kv_put.cpp
    kv_range.cpp
    kv_txn_compare.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_result
)

END()

RECURSE_FOR_TESTS(
    ut
)
