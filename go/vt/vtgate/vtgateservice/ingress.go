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
	ingressBytesKey        struct{}
	ingressBytesByQueryKey struct{}
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

// ContextWithIngressBytesByQuery stores ingress bytes by query index.
func ContextWithIngressBytesByQuery(ctx context.Context, ingressBytes []uint64) context.Context {
	values := append([]uint64(nil), ingressBytes...)
	return context.WithValue(ctx, ingressBytesByQueryKey{}, values)
}

// IngressBytesForQuery returns ingress bytes for a query index.
func IngressBytesForQuery(ctx context.Context, index int) (uint64, bool) {
	ingressBytes, ok := ctx.Value(ingressBytesByQueryKey{}).([]uint64)
	if !ok || index < 0 || index >= len(ingressBytes) {
		return 0, false
	}
	return ingressBytes[index], true
}

// AllocateQueryIngressBytes distributes ingress bytes across queries by weight.
func AllocateQueryIngressBytes(total uint64, weights []int) []uint64 {
	allocations := make([]uint64, len(weights))
	if len(weights) == 0 || total == 0 {
		return allocations
	}

	var totalWeight uint64
	for _, weight := range weights {
		if weight > 0 {
			totalWeight += uint64(weight)
		}
	}
	if totalWeight == 0 {
		allocateEvenly(total, allocations)
		return allocations
	}

	lastPositiveWeight := -1
	for i, weight := range weights {
		if weight > 0 {
			lastPositiveWeight = i
		}
	}
	var allocated uint64
	for i, weight := range weights {
		if i == lastPositiveWeight {
			allocations[i] = total - allocated
			break
		}
		if weight > 0 {
			allocations[i] = total * uint64(weight) / totalWeight
		}
		allocated += allocations[i]
	}
	return allocations
}

func allocateEvenly(total uint64, allocations []uint64) {
	base := total / uint64(len(allocations))
	remainder := total % uint64(len(allocations))
	for i := range allocations {
		allocations[i] = base
		if uint64(i) < remainder {
			allocations[i]++
		}
	}
}
