/*
Copyright 2026 The Vitess Authors.

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

package cli

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMySQLTermHandlerOnTermCancelsContexts(t *testing.T) {
	ctx, cancelCtx := context.WithCancel(context.Background())
	backgroundCtx, cancelBackgroundCtx := context.WithCancel(context.Background())

	handler := newMySQLTermHandler(cancelCtx, cancelBackgroundCtx)
	handler.onTerm()

	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.ErrorIs(t, backgroundCtx.Err(), context.Canceled)
}

func TestMySQLTermHandlerIgnoreTermsForSuppressesCancellation(t *testing.T) {
	ctx, cancelCtx := context.WithCancel(context.Background())
	backgroundCtx, cancelBackgroundCtx := context.WithCancel(context.Background())

	handler := newMySQLTermHandler(cancelCtx, cancelBackgroundCtx)
	err := handler.ignoreTermsFor(func() error {
		handler.onTerm()
		assert.NoError(t, ctx.Err())
		assert.NoError(t, backgroundCtx.Err())
		return nil
	})

	assert.NoError(t, err)
	assert.NoError(t, ctx.Err())
	assert.NoError(t, backgroundCtx.Err())

	handler.onTerm()
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.ErrorIs(t, backgroundCtx.Err(), context.Canceled)
}
