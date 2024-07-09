/*
Copyright 2019 The Vitess Authors.

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

package callerid_test

import (
	"context"
	"testing"

	"vitess.io/vitess/go/vt/callerid"

	"github.com/stretchr/testify/assert"
)

func TestCallerIDContext(t *testing.T) {
	ef := callerid.NewEffectiveCallerID("principal", "component", "subComponent")
	im := callerid.NewImmediateCallerID("username")
	ctx := callerid.NewContext(context.Background(), ef, im)

	assert.Equal(t, ef, callerid.EffectiveCallerIDFromContext(ctx))
	assert.Equal(t, im, callerid.ImmediateCallerIDFromContext(ctx))

	assert.Nil(t, callerid.EffectiveCallerIDFromContext(context.Background()))
	assert.Nil(t, callerid.ImmediateCallerIDFromContext(context.Background()))
}

func BenchmarkCallerIDContext(b *testing.B) {
	ef := callerid.NewEffectiveCallerID("principal", "component", "subComponent")
	im := callerid.NewImmediateCallerID("username")

	b.Run("New", func(b *testing.B) {
		b.ReportAllocs()
		baseCtx := context.Background()
		for range b.N {
			_ = callerid.NewContext(baseCtx, ef, im)
		}
	})

	b.Run("EffectiveCallerIDFromContext", func(b *testing.B) {
		b.ReportAllocs()
		ctx := callerid.NewContext(context.Background(), ef, im)
		for range b.N {
			_ = callerid.EffectiveCallerIDFromContext(ctx)
		}
	})

	b.Run("ImmediateCallerIDFromContext", func(b *testing.B) {
		b.ReportAllocs()
		ctx := callerid.NewContext(context.Background(), ef, im)
		for range b.N {
			_ = callerid.EffectiveCallerIDFromContext(ctx)
		}
	})
}
