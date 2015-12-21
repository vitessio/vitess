// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package trace

import (
	"testing"

	"golang.org/x/net/context"
)

func TestFakeSpan(t *testing.T) {
	ctx := context.Background()
	RegisterSpanFactory(fakeSpanFactory{})

	// It should be safe to call all the usual methods as if a plugin were installed.
	span := NewSpanFromContext(ctx)
	span.StartLocal("label")
	span.StartClient("label")
	span.StartServer("label")
	span.Annotate("key", "value")
	span.Finish()
	NewContext(ctx, span)
	CopySpan(ctx, ctx)
}
