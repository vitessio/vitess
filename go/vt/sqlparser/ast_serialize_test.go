package sqlparser

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/test/utils"
)

var sql = "select distinct 42 as foobar from dual"
var stmt, _ = Parse(sql)
var btes, _ = Serialize(stmt)

func TestFirstRoundTrip(t *testing.T) {
	var mustMatch = utils.MustMatchFn(
		[]interface{}{ // types with unexported fields
			TableIdent{},
		},
		[]string{}, // ignored fields
	)

	bytes, err := Serialize(stmt)
	require.NoError(t, err)

	assert.LessOrEqual(t, len(bytes), len(sql), "byte size vs query size")
	fmt.Println(hex.Dump(bytes))
	output := Deserialize(bytes)
	require.NoError(t, err)
	require.NotNil(t, output)

	mustMatch(t, stmt, output, "serialize round trip")
}
func BenchmarkParse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stmt, _ := Parse(sql)
		if stmt == nil {
			b.Fail()
		}
	}
}
func BenchmarkDeserialize(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stmt := Deserialize(btes)
		if stmt == nil {
			b.Fail()
		}
	}
}
