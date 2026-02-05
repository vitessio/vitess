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

package cache

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/grpc/metadata"

	"vitess.io/vitess/go/vt/log"
)

var (
	cacheRefreshHeader          string
	cacheRefreshGRPCMetadataKey string
)

// SetCacheRefreshKey sets the global HTTP header and gRPC metadata keys for
// requests to force a cache refresh. Any whitespace characters are replaced
// with hyphens.
//
// It is not threadsafe, and should be called only at startup or in an init
// function.
func SetCacheRefreshKey(k string) {
	l := strings.ToLower(k)
	l = strings.ReplaceAll(l, " ", "-")
	cacheRefreshHeader, cacheRefreshGRPCMetadataKey = fmt.Sprintf("x-%s", l), l
}

// NewIncomingRefreshContext returns an incoming gRPC context with metadata
// set to signal a cache refresh.
func NewIncomingRefreshContext(ctx context.Context) context.Context {
	md := metadata.Pairs(cacheRefreshGRPCMetadataKey, "true")
	return metadata.NewIncomingContext(ctx, md)
}

// ShouldRefreshFromIncomingContext returns true if the gRPC metadata in the
// incoming context signals a cache refresh was requested.
func ShouldRefreshFromIncomingContext(ctx context.Context) bool {
	if cacheRefreshGRPCMetadataKey == "" {
		return false
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}

	vals := md.Get(cacheRefreshGRPCMetadataKey)
	if len(vals) == 0 {
		return false
	}

	shouldRefresh, err := strconv.ParseBool(vals[0])
	if err != nil {
		log.Warningf("failed to parse %s metadata key as bool: %s", cacheRefreshGRPCMetadataKey, err)
		return false
	}

	return shouldRefresh
}

// ShouldRefreshFromRequest returns true if the HTTP request headers signal a
// cache refresh was requested.
func ShouldRefreshFromRequest(r *http.Request) bool {
	if cacheRefreshHeader == "" {
		return false
	}

	h := r.Header.Get(cacheRefreshHeader)
	if h == "" {
		return false
	}

	shouldRefresh, err := strconv.ParseBool(h)
	if err != nil {
		log.Warningf("failed to parse %s header as bool: %s", cacheRefreshHeader, err)
		return false
	}

	return shouldRefresh
}
