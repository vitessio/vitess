package engine

import (
	"fmt"
	"sort"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var noUsedTables = []string{}

func collectSortedUniqueStrings() (add func(string), collect func() []string) {
	uniq := make(map[string]any)
	add = func(v string) {
		uniq[v] = nil
	}
	collect = func() []string {
		sorted := make([]string, 0, len(uniq))
		for v := range uniq {
			sorted = append(sorted, v)
		}
		sort.Strings(sorted)
		return sorted
	}

	return add, collect
}

func concatSortedUniqueStringSlices() (add func([]string), collect func() []string) {
	subadd, collect := collectSortedUniqueStrings()
	add = func(vs []string) {
		for _, v := range vs {
			subadd(v)
		}
	}
	return add, collect
}

func singleQualifiedIdentifier(ks *vindexes.Keyspace, i sqlparser.IdentifierCS) []string {
	return singleQualifiedString(ks, i.String())
}

func singleQualifiedTableName(ks *vindexes.Keyspace, t sqlparser.TableName) []string {
	return singleQualifiedIdentifier(ks, t.Name)
}

func singleQualifiedString(ks *vindexes.Keyspace, s string) []string {
	return []string{qualifiedString(ks, s)}
}

func qualifiedIdentifier(ks *vindexes.Keyspace, i sqlparser.IdentifierCS) string {
	return qualifiedString(ks, i.String())
}

func qualifiedString(ks *vindexes.Keyspace, s string) string {
	return fmt.Sprintf("%s.%s", ks.Name, s)
}
func qualifiedTableName(ks *vindexes.Keyspace, t sqlparser.TableName) string {
	return qualifiedIdentifier(ks, t.Name)
}
