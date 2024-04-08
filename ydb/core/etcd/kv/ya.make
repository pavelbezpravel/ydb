LIBRARY()

SRCS(
    kv_delete.cpp
    kv_put.cpp
    kv_range.cpp
    kv_table_creator.cpp
    kv_txn.cpp
    kv.cpp
)

PEERDIR(
    ydb/core/etcd/revision
    ydb/library/actors/core
    ydb/library/query_actor
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_result
)

END()
