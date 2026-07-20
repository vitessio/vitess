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

package servenv

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"google.golang.org/grpc/metadata"
)

// newBenchStaticAuthPlugin builds a plugin with numEntries entries; the target
// user is the last entry (worst case for the linear scan). If hashed is true
// the target entry uses CachingSha2Password, otherwise plaintext Password.
func newBenchStaticAuthPlugin(numEntries int, hashed bool, password string) *StaticAuthPlugin {
	entries := make([]staticAuthEntry, 0, numEntries)
	for i := 0; i < numEntries-1; i++ {
		entries = append(entries, staticAuthEntry{
			StaticAuthConfigEntry: StaticAuthConfigEntry{
				Username: fmt.Sprintf("other_user_%d", i),
				Password: "other_password",
			},
		})
	}
	target := staticAuthEntry{
		StaticAuthConfigEntry: StaticAuthConfigEntry{
			Username: "bench_user",
		},
	}
	if hashed {
		stage1 := sha256.Sum256([]byte(password))
		hash := sha256.Sum256(stage1[:])
		target.CachingSha2Password = hex.EncodeToString(hash[:])
		target.cachingSha2Password = hash[:]
	} else {
		target.Password = password
	}
	entries = append(entries, target)
	return &StaticAuthPlugin{entries: entries}
}

func benchmarkStaticAuthPluginAuthenticate(b *testing.B, hashed bool) {
	plugin := newBenchStaticAuthPlugin(5, hashed, "bench_password")
	md := metadata.New(map[string]string{
		"username": "bench_user",
		"password": "bench_password",
	})
	ctx := metadata.NewIncomingContext(b.Context(), md)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := plugin.Authenticate(ctx, "/test.Service/Method"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStaticAuthPlugin_AuthenticatePlaintext measures per-RPC
// authentication cost with a plaintext Password entry.
func BenchmarkStaticAuthPlugin_AuthenticatePlaintext(b *testing.B) {
	benchmarkStaticAuthPluginAuthenticate(b, false)
}

// BenchmarkStaticAuthPlugin_AuthenticateHashed measures per-RPC authentication
// cost with a CachingSha2Password entry.
func BenchmarkStaticAuthPlugin_AuthenticateHashed(b *testing.B) {
	benchmarkStaticAuthPluginAuthenticate(b, true)
}
