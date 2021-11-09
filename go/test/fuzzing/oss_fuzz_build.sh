#!/bin/bash

# Copyright 2021 The Vitess Authors.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o nounset
set -o pipefail
set -o errexit
set -x

go get github.com/AdaLogics/go-fuzz-headers
go mod vendor

#consistent_lookup_test.go is needed for loggingVCursor
mv ./go/vt/vtgate/vindexes/consistent_lookup_test.go \
   ./go/vt/vtgate/vindexes/consistent_lookup_test_fuzz.go
compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/vindexes FuzzVindex fuzz_vindex

# fake_vcursor_test.go is needed for loggingVCursor
mv ./go/vt/vtgate/engine/fake_vcursor_test.go \
    ./go/vt/vtgate/engine/fake_vcursor.go
compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/engine FuzzEngine engine_fuzzer

# plan_test.go is needed for vschemaWrapper
mv ./go/vt/vtgate/planbuilder/plan_test.go \
    ./go/vt/vtgate/planbuilder/plan_test_fuzz.go
compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/planbuilder FuzzTestBuilder fuzz_test_builder gofuzz


compile_go_fuzzer vitess.io/vitess/go/test/fuzzing Fuzz vtctl_fuzzer
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzIsDML is_dml_fuzzer
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzNormalizer normalizer_fuzzer
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzParser parser_fuzzer
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzGRPCTMServer fuzz_grpc_tm_server
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzOnlineDDLFromCommentedStatement fuzz_online_ddl_from_commented_statement
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzNewOnlineDDLs fuzz_new_online_ddls
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzEqualsSQLNode fuzz_equals_sql_node

compile_go_fuzzer vitess.io/vitess/go/mysql FuzzWritePacket write_packet_fuzzer
compile_go_fuzzer vitess.io/vitess/go/mysql FuzzHandleNextCommand handle_next_command_fuzzer
compile_go_fuzzer vitess.io/vitess/go/mysql FuzzReadQueryResults read_query_results_fuzzer
compile_go_fuzzer vitess.io/vitess/go/mysql FuzzTLSServer fuzz_tls

# Several test utils are needed from suite_test.go:
mv ./go/vt/vtgate/grpcvtgateconn/suite_test.go \
   ./go/vt/vtgate/grpcvtgateconn/suite_test_fuzz.go
mv ./go/vt/vtgate/grpcvtgateconn/fuzz_flaky_test.go \
   ./go/vt/vtgate/grpcvtgateconn/fuzz.go
compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/grpcvtgateconn Fuzz grpc_vtgate_fuzzer

compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/planbuilder/abstract FuzzAnalyse fuzz_analyse gofuzz



# Build dictionaries
cp $SRC/vitess/go/test/fuzzing/vtctl_fuzzer.dict $OUT/
