LIBRARY()

SRCS(
    revision_inc.cpp
    revision_table_creator.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/query_actor
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_result
)

END()
