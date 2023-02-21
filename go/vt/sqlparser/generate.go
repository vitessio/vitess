package sqlparser

// Generate all the AST helpers using the tooling in `go/tools`

//go:generate go run ./goyacc -fo sql.go sql.y
//go:generate go run ../../tools/asthelpergen/main  --in . --iface vitess.io/vitess/go/vt/sqlparser.SQLNode --clone_exclude "*ColName" --equals_custom "*ColName"
//go:generate go run ../../tools/astfmtgen vitess.io/vitess/go/vt/sqlparser/...
