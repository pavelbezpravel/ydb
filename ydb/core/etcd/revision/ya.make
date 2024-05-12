LIBRARY()

SRCS(
    revision_get.cpp
    revision_set.cpp
    revision_table_create.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/query_actor
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_result
)

END()
