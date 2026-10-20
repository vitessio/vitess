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

MAKEFLAGS = -s

all: grpcvtctldclient localvtctldclient

grpcvtctldclient:
	go generate ../$@/...
<<<<<<< HEAD
	gofmt -w ../$@/client_gen.go
||||||| parent of aa835af9bd (ci: build the Go module and build caches in a dedicated warm workflow (#21614))
	go tool -modfile=../../../../tools/gofumpt/go.mod gofumpt -w ../$@/client_gen.go
=======
	go run -trimpath -modfile=../../../../tools/gofumpt/go.mod mvdan.cc/gofumpt -w ../$@/client_gen.go
>>>>>>> aa835af9bd (ci: build the Go module and build caches in a dedicated warm workflow (#21614))

localvtctldclient:
	go generate ../$@/...
<<<<<<< HEAD
	gofmt -w ../$@/client_gen.go
||||||| parent of aa835af9bd (ci: build the Go module and build caches in a dedicated warm workflow (#21614))
	go tool -modfile=../../../../tools/gofumpt/go.mod gofumpt -w ../$@/client_gen.go
=======
	go run -trimpath -modfile=../../../../tools/gofumpt/go.mod mvdan.cc/gofumpt -w ../$@/client_gen.go
>>>>>>> aa835af9bd (ci: build the Go module and build caches in a dedicated warm workflow (#21614))
