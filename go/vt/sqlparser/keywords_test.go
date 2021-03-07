package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeywordTable(t *testing.T) {
	for _, kw := range keywords {
		lookup, ok := keywordLookupTable.LookupString(kw.name)
		require.Truef(t, ok, "keyword %q failed to match", kw.name)
		require.Equalf(t, lookup, kw.id, "keyword %q matched to %d (expected %d)", kw.name, lookup, kw.id)
	}
}
