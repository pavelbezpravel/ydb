LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/public/issue
    ydb/public/api/protos
)

END()
