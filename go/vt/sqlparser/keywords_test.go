package sqlparser

import "testing"

func TestKeywordTable(t *testing.T) {
	for _, kw := range keywords {
		lookup, ok := keywordLookupTable.LookupString(kw.name)
		if !ok {
			t.Fatalf("keyword %q failed to match", kw.name)
		}
		if lookup != kw.id {
			t.Fatalf("keyword %q matched to %d (expected %d)", kw.name, lookup, kw.id)
		}
	}
}
