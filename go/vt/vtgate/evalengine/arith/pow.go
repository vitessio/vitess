package arith

import (
	"math/big"
	"sync"
	"sync/atomic"
)

const (
	// PowTabLen is the largest cached power for integers.
	PowTabLen = 20

	// BigPowTabLen is the largest cached power for *big.Ints.
	BigPowTabLen = 1e5
)

var (
	pow10tab = [PowTabLen]uint64{
		0:  1,
		1:  10,
		2:  100,
		3:  1000,
		4:  10000,
		5:  100000,
		6:  1000000,
		7:  10000000,
		8:  100000000,
		9:  1000000000,
		10: 10000000000,
		11: 100000000000,
		12: 1000000000000,
		13: 10000000000000,
		14: 100000000000000,
		15: 1000000000000000,
		16: 10000000000000000,
		17: 100000000000000000,
		18: 1000000000000000000,
		19: 10000000000000000000,
	}
	bigMu       sync.Mutex // protects writes to bigPow10Tab
	bigPow10Tab atomic.Value
)

func loadBigTable() []*big.Int {
	return *(bigPow10Tab.Load().(*[]*big.Int))
}

func storeBigTable(x *[]*big.Int) {
	bigPow10Tab.Store(x)
}

var OneInt = big.NewInt(1)
var TenInt = big.NewInt(10)

// PowOfTenBig reports whether x is a power of 10.
func PowOfTenBig(x *big.Int) bool {
	if x.Bit(0) != 0 {
		return x.Cmp(OneInt) == 0
	}
	if x.Sign() == 0 {
		return true
	}
	q := new(big.Int).Set(x)
	r := new(big.Int)
	for len := BigLength(x); len > 20; len-- {
		q.QuoRem(q, TenInt, r)
		if r.Sign() != 0 {
			return false
		}
	}
	return PowOfTen(q.Uint64())
}

// PowOfTen reports whether x is a power of 10.
func PowOfTen(x uint64) bool {
	if x&1 != 0 {
		return x == 1
	}
	switch x {
	case 10,
		100,
		1000,
		10000,
		100000,
		1000000,
		10000000,
		100000000,
		1000000000,
		10000000000,
		100000000000,
		1000000000000,
		10000000000000,
		100000000000000,
		1000000000000000,
		10000000000000000,
		100000000000000000,
		1000000000000000000,
		10000000000000000000:
		return true
	default:
		return false
	}
}

// BigPow10 computes 10**n.
//
// The returned *big.Int must not be modified.
func BigPow10(n uint64) *big.Int {
	tab := loadBigTable()

	tabLen := uint64(len(tab))
	if n < tabLen {
		return tab[n]
	}

	// Too large for our table.
	if n >= BigPowTabLen {
		// As an optimization, we don't need to start from
		// scratch each time. Start from the largest term we've
		// found so far.
		partial := tab[tabLen-1]
		p := new(big.Int).SetUint64(n - (tabLen - 1))
		return p.Mul(partial, p.Exp(TenInt, p, nil))
	}
	return growBigTen(n)
}

func growBigTen(n uint64) *big.Int {
	// We need to expand our table to contain the value for
	// 10**n.
	bigMu.Lock()

	tab := loadBigTable()

	// Look again in case the table was rebuilt before we grabbed
	// the lock.
	tableLen := uint64(len(tab))
	if n < tableLen {
		bigMu.Unlock()
		return tab[n]
	}

	// n < BigTabLen

	newLen := tableLen * 2
	for newLen <= n {
		newLen *= 2
	}
	if newLen > BigPowTabLen {
		newLen = BigPowTabLen
	}
	for i := tableLen; i < newLen; i++ {
		tab = append(tab, new(big.Int).Mul(tab[i-1], TenInt))
	}

	storeBigTable(&tab)
	bigMu.Unlock()
	return tab[n]
}

func Safe(e uint64) bool {
	return e < PowTabLen
}

// Pow10 returns 10**e and a boolean indicating whether the
// result fits into a uint64.
func Pow10(e uint64) (uint64, bool) {
	if e < PowTabLen {
		return pow10tab[e], true
	}
	return 0, false
}

// Pow10Int returns 10**e and a boolean indicating whether the
// result fits into an int64.
func Pow10Int(e uint64) (int64, bool) {
	if e < PowTabLen-1 {
		return int64(pow10tab[e]), true
	}
	return 0, false
}

func init() {
	// Can we move this into a var decl without copylock freaking out?
	storeBigTable(&[]*big.Int{
		0:  new(big.Int).SetUint64(1),
		1:  TenInt,
		2:  new(big.Int).SetUint64(100),
		3:  new(big.Int).SetUint64(1000),
		4:  new(big.Int).SetUint64(10000),
		5:  new(big.Int).SetUint64(100000),
		6:  new(big.Int).SetUint64(1000000),
		7:  new(big.Int).SetUint64(10000000),
		8:  new(big.Int).SetUint64(100000000),
		9:  new(big.Int).SetUint64(1000000000),
		10: new(big.Int).SetUint64(10000000000),
		11: new(big.Int).SetUint64(100000000000),
		12: new(big.Int).SetUint64(1000000000000),
		13: new(big.Int).SetUint64(10000000000000),
		14: new(big.Int).SetUint64(100000000000000),
		15: new(big.Int).SetUint64(1000000000000000),
		16: new(big.Int).SetUint64(10000000000000000),
		17: new(big.Int).SetUint64(100000000000000000),
		18: new(big.Int).SetUint64(1000000000000000000),
		19: new(big.Int).SetUint64(10000000000000000000),
	})
}
