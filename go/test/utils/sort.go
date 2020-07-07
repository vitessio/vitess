package utils

import (
	"sort"
	"strings"
)

//SortString sorts the string.
func SortString(w string) string {
	s := strings.Split(w, "")
	sort.Strings(s)
	return strings.Join(s, "")
}
