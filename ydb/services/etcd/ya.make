LIBRARY()

SRCS(
    api/kv/grpc_service.cpp
)

PEERDIR(
    ydb/public/api/etcd
    ydb/library/grpc/server
    ydb/core/grpc_services
    ydb/core/grpc_services/base
)

END()
