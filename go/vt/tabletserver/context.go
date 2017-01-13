// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"golang.org/x/net/context"
)

type localContextKey int

func localContext() context.Context {
	return context.WithValue(context.Background(), localContextKey(0), 0)
}

func isLocalContext(ctx context.Context) bool {
	return ctx.Value(localContextKey(0)) != nil
}
