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
