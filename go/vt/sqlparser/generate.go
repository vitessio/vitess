/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

// Generate all the AST helpers using the tooling in `go/tools`

<<<<<<< HEAD
//go:generate go run ./goyacc -fo sql.go sql.y
||||||| parent of aa835af9bd (ci: build the Go module and build caches in a dedicated warm workflow (#21614))
//go:generate go tool -modfile=../../../tools/goyacc/go.mod goyacc -o sql.go sql.y
//go:generate go tool -modfile=../../../tools/goimports/go.mod goimports -local vitess.io/vitess -w sql.go
//go:generate go tool -modfile=../../../tools/gofumpt/go.mod gofumpt -w sql.go
=======
//go:generate go run -trimpath -modfile=../../../tools/goyacc/go.mod github.com/vitessio/goyacc -o sql.go sql.y
//go:generate go run -trimpath -modfile=../../../tools/goimports/go.mod golang.org/x/tools/cmd/goimports -local vitess.io/vitess -w sql.go
//go:generate go run -trimpath -modfile=../../../tools/gofumpt/go.mod mvdan.cc/gofumpt -w sql.go
>>>>>>> aa835af9bd (ci: build the Go module and build caches in a dedicated warm workflow (#21614))
//go:generate go run ../../tools/asthelpergen/main  --in . --iface vitess.io/vitess/go/vt/sqlparser.SQLNode --clone_exclude "*ColName" --equals_custom "*ColName"
//go:generate go run ../../tools/astfmtgen vitess.io/vitess/go/vt/sqlparser/...
