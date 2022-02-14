package sqlparser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkWalkLargeExpression(b *testing.B) {
	for i := 0; i < 10; i++ {
		b.Run(fmt.Sprintf("%d", i), func(b *testing.B) {
			exp := newGenerator(int64(i*100), 5).expression()
			count := 0
			for i := 0; i < b.N; i++ {
				err := Walk(func(node SQLNode) (kontinue bool, err error) {
					count++
					return true, nil
				}, exp)
				require.NoError(b, err)
			}
		})
	}
}

func BenchmarkRewriteLargeExpression(b *testing.B) {
	for i := 1; i < 7; i++ {
		b.Run(fmt.Sprintf("%d", i), func(b *testing.B) {
			exp := newGenerator(int64(i*100), i).expression()
			count := 0
			for i := 0; i < b.N; i++ {
				_ = Rewrite(exp, func(_ *Cursor) bool {
					count++
					return true
				}, func(_ *Cursor) bool {
					count--
					return true
				})
			}
		})
	}
}
