package vindexes

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/testutil"
	"vitess.io/vitess/go/sqltypes"
)

type GoldenUnicodeHash struct {
	Input  []byte
	XXHash []byte
	MD5    []byte
}

func GenerateGoldenCases(t *testing.T, inputs [][]byte, outfile string) {
	var goldenHash []GoldenUnicodeHash

	for _, input := range inputs {
		v := sqltypes.NewVarChar(string(input))

		hash1, err := unicodeHash(&collateXX, v)
		require.NoError(t, err)

		hash2, err := unicodeHash(&collateMD5, v)
		require.NoError(t, err)

		goldenHash = append(goldenHash, GoldenUnicodeHash{
			Input:  input,
			XXHash: hash1,
			MD5:    hash2,
		})
	}

	f, err := os.Create(outfile)
	require.NoError(t, err)
	defer f.Close()

	w := json.NewEncoder(f)
	w.SetIndent("", "  ")
	w.SetEscapeHTML(false)
	w.Encode(goldenHash)
}

const GenerateUnicodeHashes = false

func TestUnicodeHashGenerate(t *testing.T) {
	if !GenerateUnicodeHashes {
		t.Skipf("Do not generate")
	}

	golden := &testutil.GoldenTest{}
	if err := golden.DecodeFromFile("../../../mysql/collations/testdata/wiki_416c626572742045696e737465696e.gob.gz"); err != nil {
		t.Fatal(err)
	}

	var inputs [][]byte
	for _, tc := range golden.Cases {
		inputs = append(inputs, tc.Text)
	}
	GenerateGoldenCases(t, inputs, "testdata/unicode_hash_golden.json")
}

func loadGoldenUnicodeHash(t testing.TB) []GoldenUnicodeHash {
	var golden []GoldenUnicodeHash

	f, err := os.Open("testdata/unicode_hash_golden.json")
	require.NoError(t, err)

	err = json.NewDecoder(f).Decode(&golden)
	require.NoError(t, err)

	return golden
}

func TestUnicodeHash(t *testing.T) {
	for _, tc := range loadGoldenUnicodeHash(t) {
		v := sqltypes.NewVarChar(string(tc.Input))

		hash1, err := unicodeHash(&collateXX, v)
		require.NoError(t, err)
		assert.Equal(t, tc.XXHash, hash1)

		hash2, err := unicodeHash(&collateMD5, v)
		require.NoError(t, err)
		assert.Equal(t, tc.MD5, hash2)
	}
}

func BenchmarkUnicodeTest(b *testing.B) {
	for _, repeat := range []int{1, 4, 16} {
		var values []sqltypes.Value
		for _, g := range loadGoldenUnicodeHash(b) {
			vv := strings.Repeat(string(g.Input), repeat)
			values = append(values, sqltypes.NewVarChar(vv))
		}

		b.Run(fmt.Sprintf("repeat=%d", repeat), func(b *testing.B) {
			b.Run("MD5", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					_, _ = unicodeHash(&collateMD5, values[25])
				}
			})

			b.Run("xxHash", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					_, _ = unicodeHash(&collateXX, values[25])
				}
			})
		})
	}
}
