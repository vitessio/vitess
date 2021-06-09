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

export WORKDIR=$SRC/vitess
if [ "$SANITIZER" = "coverage" ]
then

  compile_go_fuzzer()
  {
  path=$1
  function=$2
  fuzzer=$3
  tags="-tags gofuzz"
  if [[ $#  -eq 4 ]]; then
    tags="-tags $4"
  fi

  go mod download
  fuzzed_package=`go list $tags -f '{{.Name}}' $path`
  abspath=`go list $tags -f {{.Dir}} $path`
  cd $abspath
  cp $GOPATH/ossfuzz_coverage_runner.go ./"${function,,}"_test.go
  sed -i -e 's/FuzzFunction/'$function'/' ./"${function,,}"_test.go
  sed -i -e 's/mypackagebeingfuzzed/'$fuzzed_package'/' ./"${function,,}"_test.go
  sed -i -e 's/TestFuzzCorpus/Test'$function'Corpus/' ./"${function,,}"_test.go

  fuzzed_repo=$(go list $tags -f {{.Module}} "$path")
  abspath_repo=`go list -m $tags -f {{.Dir}} $fuzzed_repo || go list $tags -f {{.Dir}} $fuzzed_repo`
  echo "s=$fuzzed_repo"="$abspath_repo"= > $OUT/$fuzzer.gocovpath
  go test -run Test${function}Corpus -v $tags -coverpkg $fuzzed_repo/... -c -o $OUT/$fuzzer $path
  } 
else
  # Change name in case we are not build with coverage
  mv $WORKDIR/go/vt/vtgate/grpcvtgateconn/fuzz_flaky_test.go \
     $WORKDIR/go/vt/vtgate/grpcvtgateconn/fuzz.go
  mv $WORKDIR/go/vt/vtgate/grpcvtgateconn/suite_test.go \
     $WORKDIR/go/vt/vtgate/grpcvtgateconn/suite_fuzz.go

  mv $WORKDIR/go/vt/vtgate/engine/fuzz_flaky_test.go \
     $WORKDIR/go/vt/vtgate/engine/fuzz.go
  mv $WORKDIR/go/vt/vtgate/engine/fake_vcursor_test.go \
     $WORKDIR/go/vt/vtgate/engine/fake_vcursor_fuzz.go
fi


compile_go_fuzzer vitess.io/vitess/go/test/fuzzing Fuzz vtctl_fuzzer
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzIsDML is_dml_fuzzer
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzNormalizer normalizer_fuzzer
compile_go_fuzzer vitess.io/vitess/go/test/fuzzing FuzzParser parser_fuzzer
compile_go_fuzzer vitess.io/vitess/go/mysql FuzzWritePacket write_packet_fuzzer
compile_go_fuzzer vitess.io/vitess/go/mysql FuzzHandleNextCommand handle_next_command_fuzzer
compile_go_fuzzer vitess.io/vitess/go/mysql FuzzReadQueryResults read_query_results_fuzzer
compile_go_fuzzer vitess.io/vitess/go/mysql FuzzTLSServer fuzz_tls
compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/grpcvtgateconn Fuzz grpc_vtgate_fuzzer
compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/planbuilder FuzzAnalyse planbuilder_fuzzer
compile_go_fuzzer vitess.io/vitess/go/vt/vtgate/engine FuzzEngine engine_fuzzer

# Build dictionaries
cp $SRC/vitess/go/test/fuzzing/vtctl_fuzzer.dict $OUT/

