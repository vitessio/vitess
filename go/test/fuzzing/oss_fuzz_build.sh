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

compile_go_fuzzer ./go/test/fuzzing Fuzz vtctl_fuzzer
compile_go_fuzzer ./go/test/fuzzing FuzzIsDML is_dml_fuzzer
compile_go_fuzzer ./go/test/fuzzing FuzzNormalizer normalizer_fuzzer
compile_go_fuzzer ./go/test/fuzzing FuzzParser parser_fuzzer

#cp ./go/test/fuzzing/mysql/mysql_fuzzer.go ./go/mysql/
compile_go_fuzzer ./go/mysql FuzzWritePacket write_packet_fuzzer
compile_go_fuzzer ./go/mysql FuzzHandleNextCommand handle_next_command_fuzzer
compile_go_fuzzer ./go/mysql FuzzReadQueryResults read_query_results_fuzzer

# Build dictionaries
cp $SRC/vitess/go/test/fuzzing/vtctl_fuzzer.dict $OUT/
