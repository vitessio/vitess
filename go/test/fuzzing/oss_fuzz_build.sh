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

if [ "$SANITIZER" = "coverage" ]
then
        # As of now Vitess cannot be compiled with coverage sanitizer
        exit 0
fi

compile_go_fuzzer ./go/test/fuzzing Fuzz vtctl_fuzzer
compile_go_fuzzer ./go/test/fuzzing FuzzIsDML is_dml_fuzzer
compile_go_fuzzer ./go/test/fuzzing FuzzNormalizer normalizer_fuzzer
compile_go_fuzzer ./go/test/fuzzing FuzzParser parser_fuzzer

compile_go_fuzzer ./go/mysql FuzzWritePacket write_packet_fuzzer
compile_go_fuzzer ./go/mysql FuzzHandleNextCommand handle_next_command_fuzzer
compile_go_fuzzer ./go/mysql FuzzReadQueryResults read_query_results_fuzzer
compile_go_fuzzer ./go/mysql FuzzTLSServer fuzz_tls
compile_go_fuzzer ./go/vt/vtgate/grpcvtgateconn Fuzz grpc_vtgate_fuzzer
compile_go_fuzzer ./go/vt/vtgate/planbuilder FuzzAnalyse planbuilder_fuzzer

mv ./go/vt/vtgate/engine/fake_vcursor_test.go \
	./go/vt/vtgate/engine/fake_vcursor.go
compile_go_fuzzer ./go/vt/vtgate/engine FuzzEngine engine_fuzzer

# Build dictionaries
cp $SRC/vitess/go/test/fuzzing/vtctl_fuzzer.dict $OUT/
