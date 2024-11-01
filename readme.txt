
//~/grpcurl -plaintext -d '{"is_id": 12345}' 127.0.0.1:8012 inventory.Invsvc/GetInventory
//protoc --go_out=. --go-grpc_out=. invsvc.proto
//export PATH="$PATH:$(go env GOPATH)/bin"