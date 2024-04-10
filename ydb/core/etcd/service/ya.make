LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    ydb/core/etcd/kv
    ydb/core/etcd/revision
    ydb/library/actors/core
)

END()
