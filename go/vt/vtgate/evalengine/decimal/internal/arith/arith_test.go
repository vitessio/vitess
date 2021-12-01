package arith

import (
	"math"
	"math/big"
	"strings"
	"testing"
)

func B(s string) *big.Int {
	x, _ := new(big.Int).SetString(s, 10)
	return x
}

var testCases = [...]struct {
	x *big.Int
	y uint64
}{
	{B("18446744073709551616"), 1},
	{B("18446744073709551617"), 3},
	{B("18446744073709551617"), math.MaxUint64},
	{B("36893488147419103230"), math.MaxUint64},
	{B("36893488147419103230"), 42},
	{B("221360928884514619380432421349238409238490238423423"), math.MaxUint64},
	{B(strings.Repeat("2", 500)), math.MaxUint64},
}

func testFn(t *testing.T,
	fn1 func(z, x, y *big.Int) *big.Int,
	fn2 func(z, x *big.Int, y uint64) *big.Int,
) {
	for _, tc := range testCases {
		x := new(big.Int).Set(tc.x)
		y0 := new(big.Int).SetUint64(tc.y)
		r := fn1(y0, x, y0)

		r0 := fn2(new(big.Int), x, tc.y)
		if r.Cmp(r0) != 0 {
			t.Fatalf(`
r : %s
r0: %s`, r, r0)
		}

		r1 := fn2(x, x, tc.y)
		if r.Cmp(r1) != 0 {
			t.Fatalf(`
r : %s
r1: %s`, r, r1)
		}
	}
}

func TestAdd(t *testing.T) { testFn(t, (*big.Int).Add, Add) }
func TestSub(t *testing.T) { testFn(t, (*big.Int).Sub, Sub) }
func TestMul(t *testing.T) { testFn(t, (*big.Int).Mul, Mul) }

func benchmarkFn(b *testing.B, fn func(z, x *big.Int, y uint64) *big.Int) {
	var z big.Int
	for i := 0; i < b.N; i++ {
		tc := testCases[i%len(testCases)]
		fn(&z, tc.x, tc.y)
	}
}

func BenchmarkAdd(b *testing.B) { benchmarkFn(b, Add) }
func BenchmarkSub(b *testing.B) { benchmarkFn(b, Sub) }
func BenchmarkMul(b *testing.B) { benchmarkFn(b, Mul) }
