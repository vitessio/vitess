//go:build gofuzz
// +build gofuzz

package fuzzing

import (
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"

	fuzz "github.com/AdaLogics/go-fuzz-headers"
)

func FuzzIsDML(data []byte) int {
	_ = sqlparser.IsDML(string(data))
	return 1
}

func FuzzNormalizer(data []byte) int {
	stmt, reservedVars, err := sqlparser.Parse2(string(data))
	if err != nil {
		return -1
	}
	bv := make(map[string]*querypb.BindVariable)
	sqlparser.Normalize(stmt, sqlparser.NewReservedVars("bv", reservedVars), bv)
	return 1
}

func FuzzParser(data []byte) int {
	_, err := sqlparser.Parse(string(data))
	if err != nil {
		return 0
	}
	return 1
}

func FuzzNodeFormat(data []byte) int {
	f := fuzz.NewConsumer(data)
	query, err := f.GetSQLString()
	if err != nil {
		return 0
	}
	node, err := sqlparser.Parse(query)
	if err != nil {
		return 0
	}
	buf := &sqlparser.TrackedBuffer{}
	err = f.GenerateStruct(buf)
	if err != nil {
		return 0
	}
	node.Format(buf)
	return 1
}

func FuzzSplitStatementToPieces(data []byte) int {
	_, _ = sqlparser.SplitStatementToPieces(string(data))
	return 1
}
