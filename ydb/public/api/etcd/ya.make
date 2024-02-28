PROTO_LIBRARY(api-etcd)

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(3.5.10)

ORIGINAL_SOURCE(https://github.com/etcd-io/etcd/releases/download/v3.5.10/etcd-v3.5.10-linux-amd64.tar.gz)

PEERDIR(contrib/libs/gogo/protobuf)

PROTO_NAMESPACE(
    GLOBAL
    ydb/public/api
)

GRPC()
SRCS(
    api/authpb/auth.proto
    api/etcdserverpb/rpc.proto
    api/mvccpb/kv.proto
    server/etcdserver/api/v3election/v3electionpb/v3election.proto
    server/etcdserver/api/v3lock/v3lockpb/v3lock.proto
    server/lease/leasepb/lease.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

EXCLUDE_TAGS(GO_PROTO)

END()
