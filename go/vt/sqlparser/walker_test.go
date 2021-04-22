/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
