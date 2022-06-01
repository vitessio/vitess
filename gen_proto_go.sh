#!/bin/bash

go install github.com/golang/protobuf/protoc-gen-go
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess dolthub/vt/binlogdata.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess dolthub/vt/replicationdata.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess dolthub/vt/time.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess dolthub/vt/logutil.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess dolthub/vt/vtrpc.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess dolthub/vt/topodata.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess dolthub/vt/query.proto
protoc --proto_path=proto --go_out=. --go_opt=module=github.com/dolthub/vitess dolthub/vt/vtgate.proto
