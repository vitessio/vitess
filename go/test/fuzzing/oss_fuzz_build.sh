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

# Disable logging for mysql conn
# This affects the mysql fuzzers
sed -i '/log.Errorf/c\\/\/log.Errorf' $SRC/vitess/go/mysql/conn.go

mv ./go/vt/vttablet/tabletmanager/vreplication/framework_test.go \
   ./go/vt/vttablet/tabletmanager/vreplication/framework_fuzz.go

#consistent_lookup_test.go is needed for loggingVCursor
mv ./go/vt/vtgate/vindexes/consistent_lookup_test.go \
   ./go/vt/vtgate/vindexes/consistent_lookup_test_fuzz.go

# fake_vcursor_test.go is needed for loggingVCursor
mv ./go/vt/vtgate/engine/fake_vcursor_test.go \
    ./go/vt/vtgate/engine/fake_vcursor.go

# plan_test.go is needed for vschemaWrapper
mv ./go/vt/vtgate/planbuilder/plan_test.go \
    ./go/vt/vtgate/planbuilder/plan_test_fuzz.go

# tabletserver fuzzer
mv ./go/vt/vttablet/tabletserver/testutils_test.go \
   ./go/vt/vttablet/tabletserver/testutils_fuzz.go

# autogenerate and build api_marshal_fuzzer:
cd $SRC/vitess/go/vt
grep -r ') Unmarshal' .>>/tmp/marshal_targets.txt
cd $SRC/vitess/go/test/fuzzing/autogenerate
go run convert_grep_to_fuzzer.go
mv api_marshal_fuzzer.go $SRC/vitess/go/test/fuzzing/
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzAPIMarshal api_marshal_fuzzer

# collation fuzzer
mv ./go/mysql/collations/uca_test.go \
   ./go/mysql/collations/uca_test_fuzz.go

compile_go_fuzzer vitess.io/vitess/go/mysql/collations FuzzCollations fuzz_collations


compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/planbuilder FuzzTestBuilder fuzz_test_builder gofuzz
compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/vindexes FuzzVindex fuzz_vindex
compile_go_fuzzer vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication FuzzEngine fuzz_replication_engine
compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/engine FuzzEngine engine_fuzzer


compile_go_fuzzer vitess.io/vitess/go/test/fuzzing Fuzz vtctl_fuzzer
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzIsDML is_dml_fuzzer
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzNormalizer normalizer_fuzzer
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzParser parser_fuzzer
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzNodeFormat fuzz_node_format
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzGRPCTMServer fuzz_grpc_tm_server
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzOnlineDDLFromCommentedStatement fuzz_online_ddl_from_commented_statement
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzNewOnlineDDLs fuzz_new_online_ddls
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzEqualsSQLNode fuzz_equals_sql_node
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzSplitStatementToPieces fuzz_split_statement_to_pieces
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzTabletManager_ExecuteFetchAsDba fuzz_tablet_manager_execute_fetch_as_dba
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzUnmarshalJSON fuzz_tabletserver_rules_unmarshal_json
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzLoadTable fuzz_load_table


compile_go_fuzzer vitess.io/vitess/go/mysql FuzzWritePacket write_packet_fuzzer
compile_go_fuzzer vitess.io/vitess/go/mysql FuzzHandleNextCommand handle_next_command_fuzzer
compile_go_fuzzer vitess.io/vitess/go/mysql FuzzReadQueryResults read_query_results_fuzzer
compile_go_fuzzer vitess.io/vitess/go/mysql FuzzTLSServer fuzz_tls

compile_go_fuzzer vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer Fuzz vstreamer_planbuilder_fuzzer
compile_go_fuzzer vitess.io/vitess/go/vt/vttablet/tabletserver FuzzGetPlan fuzz_get_plan

# Several test utils are needed from suite_test.go:
mv ./go/vt/vtgate/grpcvtgateconn/suite_test.go \
   ./go/vt/vtgate/grpcvtgateconn/suite_test_fuzz.go
mv ./go/vt/vtgate/grpcvtgateconn/fuzz_flaky_test.go \
   ./go/vt/vtgate/grpcvtgateconn/fuzz.go
compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/grpcvtgateconn Fuzz grpc_vtgate_fuzzer

compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/planbuilder/abstract FuzzAnalyse fuzz_analyse gofuzz



# Build dictionaries
cp $SRC/vitess/go/test/fuzzing/vtctl_fuzzer.dict $OUT/
