PROTO_LIBRARY()

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(1.3.2)

ORIGINAL_SOURCE(https://github.com/gogo/protobuf/archive/v1.3.2.tar.gz)

PROTO_NAMESPACE(
    GLOBAL
    contrib/libs/gogo/protobuf
)

GRPC()

SRCS(
    gogoproto/gogo.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
