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

//go:generate go run ./goyacc -fo sql.go sql.y
//go:generate go run ../../tools/asthelpergen/main  --in . --iface vitess.io/vitess/go/vt/sqlparser.SQLNode --clone_exclude "*ColName" --equals_custom "*ColName"
//go:generate go run ../../tools/astfmtgen vitess.io/vitess/go/vt/sqlparser/...
