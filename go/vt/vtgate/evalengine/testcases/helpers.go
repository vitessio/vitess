package testcases

import "vitess.io/vitess/go/sqltypes"

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

func genSubsets1(args []string, subset []string, a, b int, yield func([]string)) {
	if a == len(subset) {
		yield(subset)
		return
	}
	if b >= len(args) {
		return
	}
	subset[a] = args[b]
	genSubsets1(args, subset, a+1, b+1, yield)
	genSubsets1(args, subset, a+0, b+1, yield)
}

func genSubsets(args []string, subsetLen int, yield func([]string)) {
	subset := make([]string, subsetLen)
	genSubsets1(args, subset, 0, 0, yield)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func mustJSON(j string) sqltypes.Value {
	v, err := sqltypes.NewJSON(j)
	if err != nil {
		panic(err)
	}
	return v
}
