#!/bin/bash

go install github.com/golang/protobuf/protoc-gen-go
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess binlogdata.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess replicationdata.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess time.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess logutil.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess vtrpc.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess topodata.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess query.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess vtgate.proto
