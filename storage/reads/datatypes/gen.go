package datatypes
//go:generate  protoc  --proto_path ../../../vendor/github.com/gogo/protobuf/:. --gofast_out=Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,plugins=grpc:. storage_common.proto
