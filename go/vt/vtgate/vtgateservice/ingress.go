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

package vtgateservice

import "context"

type (
	ingressBytesKey struct{}
)

// ContextWithIngressBytes stores ingress bytes for the query represented by ctx.
func ContextWithIngressBytes(ctx context.Context, ingressBytes uint64) context.Context {
	return context.WithValue(ctx, ingressBytesKey{}, ingressBytes)
}

// IngressBytesFromContext returns ingress bytes for the query represented by ctx.
func IngressBytesFromContext(ctx context.Context) (uint64, bool) {
	ingressBytes, ok := ctx.Value(ingressBytesKey{}).(uint64)
	return ingressBytes, ok
}
