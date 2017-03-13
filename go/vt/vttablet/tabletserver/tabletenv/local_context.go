// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletenv

import (
	"golang.org/x/net/context"
)

type localContextKey int

// LocalContext returns a context that's local to the process.
func LocalContext() context.Context {
	return context.WithValue(context.Background(), localContextKey(0), 0)
}

// IsLocalContext returns true if the context is based on LocalContext.
func IsLocalContext(ctx context.Context) bool {
	return ctx.Value(localContextKey(0)) != nil
}
