package vreplication

import "strings"

//ShardSorter implements a sort.Sort() function for sorting shard ranges
type ShardSorter []string

//Len implements the required interface for a sorting function
func (s ShardSorter) Len() int {
	return len(s)
}

//Swap implements the required interface for a sorting function
func (s ShardSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

//Key returns the prefix of a shard range
func (s ShardSorter) Key(ind int) string {
	return strings.Split(s[ind], "-")[0]
}

//Less implements the required interface for a sorting function
func (s ShardSorter) Less(i, j int) bool {
	return s.Key(i) < s.Key(j)
}
