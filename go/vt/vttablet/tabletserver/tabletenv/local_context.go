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
