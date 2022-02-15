package decimal

import (
	"math/rand"
	"strconv"
	"testing"
)

func randString() string {
	const chars = "0123456789"
	n := rand.Intn(5000)
	if n == 0 {
		n = 16
	}
	p := make([]byte, n)
	for i := range p {
		p[i] = chars[rand.Intn(len(chars))]
	}
	switch rand.Intn(3) {
	case 0:
		p = append(p, 'e', '+')
		p = strconv.AppendInt(p, int64(rand.Intn(350)), 10)
	case 1:
		p = append(p, 'e', '-')
		p = strconv.AppendInt(p, int64(rand.Intn(350)), 10)
	case 2:
		if lp := len(p); lp > 0 {
			p[rand.Intn(lp)] = '.'
		}
	}
	return string(p)
}

var benchInput = func() (a [4096]struct {
	b Big
	s string
}) {
	for i := range a {
		a[i].s = randString()
	}
	return a
}()

var globOk bool

func BenchmarkBig_SetString(b *testing.B) {
	var ok bool
	for i := 0; i < b.N; i++ {
		m := &benchInput[i%len(benchInput)]
		_, ok = m.b.SetString(m.s)
	}
	globOk = ok
}

func TestLargestForm(t *testing.T) {
	var cases = []struct {
		a, b   int
		result string
	}{
		{1, 1, "9.9"},
		{1, 0, "9"},
		{10, 10, "9999999999.9999999999"},
		{5, 5, "99999.99999"},
		{8, 0, "99999999"},
		{0, 5, "0.99999"},
	}

	for _, tc := range cases {
		var b Big
		b.LargestForm(tc.a, tc.b)
		if b.String() != tc.result {
			t.Errorf("LargestForm(%d, %d) = %q (expected %q)", tc.a, tc.b, b.String(), tc.result)
		}
	}
}
