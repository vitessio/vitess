package bloom

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/hack"
)

var (
	wordlist1 [][]byte
	n         = 1 << 16
	bf        *Bloom
)

func TestMain(m *testing.M) {
	wordlist1 = make([][]byte, n)
	for i := range wordlist1 {
		b := make([]byte, 32)
		rand.Read(b)
		wordlist1[i] = b
	}
	fmt.Println("\n###############\nbbloom_test.go")
	fmt.Print("Benchmarks relate to 2**16 OP. --> output/65536 op/ns\n###############\n\n")

	os.Exit(m.Run())
}

func TestM_NumberOfWrongs(t *testing.T) {
	bf = NewBloomFilter(float64(n*10), float64(7))

	cnt := 0
	for i := range wordlist1 {
		hash := hack.RuntimeMemhash(wordlist1[i], 0)
		if !bf.AddIfNotHas(hash) {
			cnt++
		}
	}
	fmt.Printf("Bloomfilter New(7* 2**16, 7) (-> size=%v bit): \n            Check for 'false positives': %v wrong positive 'Has' results on 2**16 entries => %v %%\n", len(bf.bitset)<<6, cnt, float64(cnt)/float64(n))

}

func TestM_JSON(t *testing.T) {
	const shallBe = int(1 << 16)

	bf = NewBloomFilter(float64(n*10), float64(7))

	cnt := 0
	for i := range wordlist1 {
		hash := hack.RuntimeMemhash(wordlist1[i], 0)
		if !bf.AddIfNotHas(hash) {
			cnt++
		}
	}

	jsonm := bf.JSONMarshal()

	// create new bloomfilter from bloomfilter's JSON representation
	bf2, err := JSONUnmarshal(jsonm)
	require.NoError(t, err)

	cnt2 := 0
	for i := range wordlist1 {
		hash := hack.RuntimeMemhash(wordlist1[i], 0)
		if !bf2.AddIfNotHas(hash) {
			cnt2++
		}
	}
	require.Equal(t, shallBe, cnt2)
}

func BenchmarkM_New(b *testing.B) {
	for r := 0; r < b.N; r++ {
		_ = NewBloomFilter(float64(n*10), float64(7))
	}
}

func BenchmarkM_Clear(b *testing.B) {
	bf = NewBloomFilter(float64(n*10), float64(7))
	for i := range wordlist1 {
		hash := hack.RuntimeMemhash(wordlist1[i], 0)
		bf.Add(hash)
	}
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		bf.Clear()
	}
}

func BenchmarkM_Add(b *testing.B) {
	bf = NewBloomFilter(float64(n*10), float64(7))
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1 {
			hash := hack.RuntimeMemhash(wordlist1[i], 0)
			bf.Add(hash)
		}
	}

}

func BenchmarkM_Has(b *testing.B) {
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1 {
			hash := hack.RuntimeMemhash(wordlist1[i], 0)
			bf.Has(hash)
		}
	}
}
