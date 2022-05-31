/*
Copyright 2022 The Vitess Authors.

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

package cache_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"vitess.io/vitess/go/vt/vtadmin/cache"
)

const refreshKey = "cache_test"

func init() {
	cache.SetCacheRefreshKey(refreshKey)
}

func TestShouldRefreshFromIncomingContext(t *testing.T) {
	t.Parallel()

	t.Run("true", func(t *testing.T) {
		t.Parallel()

		ctx := cache.NewIncomingRefreshContext(context.Background())
		assert.True(t, cache.ShouldRefreshFromIncomingContext(ctx), "incoming context with metadata set should signal refresh")
	})

	t.Run("false", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			ctx  context.Context
		}{
			{
				name: "no metadata in context",
				ctx:  context.Background(),
			},
			{
				name: "no cache key in metadata",
				ctx:  metadata.NewIncomingContext(context.Background(), metadata.Pairs("key", "val")),
			},
			{
				name: "cache key set to non-bool",
				ctx:  metadata.NewIncomingContext(context.Background(), metadata.Pairs(refreshKey, "this is not a boolean")),
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				assert.False(t, cache.ShouldRefreshFromIncomingContext(tt.ctx), "incoming context should not signal refresh")
			})
		}
	})
}

func TestShouldRefreshFromRequest(t *testing.T) {
	t.Parallel()

	t.Run("true", func(t *testing.T) {
		t.Parallel()

		r, _ := http.NewRequest("", "", nil)
		r.Header.Add("x-"+refreshKey, "true")

		assert.True(t, cache.ShouldRefreshFromRequest(r), "request header should signal cache refresh")
	})

	t.Run("false", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name    string
			headers map[string]string
		}{
			{
				name:    "no headers set",
				headers: nil,
			},
			{
				name: "no cache refresh header",
				headers: map[string]string{
					"x-forwarded-for": "whatever",
					"content-type":    "application/json",
				},
			},
			{
				name: "cache refresh set to non-boolean",
				headers: map[string]string{
					"x-" + refreshKey: "this is not a boolean",
				},
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				r, _ := http.NewRequest("", "", nil)
				for header, val := range tt.headers {
					r.Header.Add(header, val)
				}

				assert.False(t, cache.ShouldRefreshFromRequest(r), "request headers should not signal cache refresh")
			})
		}
	})
}
