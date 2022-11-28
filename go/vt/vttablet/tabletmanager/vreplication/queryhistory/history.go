package queryhistory

import (
	"fmt"
	"strings"
)

// History represents an actual sequence of SQL statements.
type History []string

// At returns the query in the history at the given index. Returns an error if
// the index is out-of-bounds.
func (h History) At(index int) (string, error) {
	if len(h) <= index || index < 0 {
		return "", fmt.Errorf("index out of range: %d", index)
	}

	return h[index], nil
}

func (h History) String() string {
	return strings.Join(h, "\n")
}
