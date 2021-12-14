package integration

import (
	"fmt"
	"strings"
	"testing"
)

func perm(a []string, f func([]string)) {
	perm1(a, f, 0)
}

func perm1(a []string, f func([]string), i int) {
	if i > len(a) {
		f(a)
		return
	}
	perm1(a, f, i+1)
	for j := i + 1; j < len(a); j++ {
		a[i], a[j] = a[j], a[i]
		perm1(a, f, i+1)
		a[i], a[j] = a[j], a[i]
	}
}

func TestAllComparisons(t *testing.T) {
	var elems = []string{"NULL", "-1", "0", "1"}
	var operators = []string{"=", "!=", "<=>", "<", "<=", ">", ">="}

	var tuples []string
	perm(elems, func(t []string) {
		tuples = append(tuples, "("+strings.Join(t, ", ")+")")
	})

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, op := range operators {
		t.Run(op, func(t *testing.T) {
			for i := 0; i < len(tuples); i++ {
				for j := 0; j < len(tuples); j++ {
					query := fmt.Sprintf("SELECT %s %s %s", tuples[i], op, tuples[j])
					local, _, localErr := safeEvaluate(query)
					if localErr != nil {
						t.Errorf("local failure: %v", localErr)
						continue
					}
					remote, remoteErr := conn.ExecuteFetch(query, 1, false)
					if remoteErr != nil {
						t.Errorf("remote failure: %v", remoteErr)
						continue
					}

					if local.Value().String() != remote.Rows[0][0].String() {
						t.Errorf("mismatch for query %q: local=%v, remote=%v", query, local.Value().String(), remote.Rows[0][0].String())
					}
				}
			}
		})
	}
}
