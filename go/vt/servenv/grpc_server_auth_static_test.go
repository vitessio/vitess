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
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// newTestStaticAuthPlugin builds a StaticAuthPlugin from config entries by
// going through the same initialization path as production code (a JSON
// credentials file loaded by staticAuthPluginInitializer).
func newTestStaticAuthPlugin(t *testing.T, entries []StaticAuthConfigEntry) *StaticAuthPlugin {
	t.Helper()

	data, err := json.Marshal(entries)
	require.NoError(t, err)

	tmpFile, err := os.CreateTemp(t.TempDir(), "static_auth_test_*.json")
	require.NoError(t, err)
	_, err = tmpFile.Write(data)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	credsFile = tmpFile.Name()
	t.Cleanup(func() { credsFile = "" })

	plugin, err := staticAuthPluginInitializer()
	require.NoError(t, err)

	staticPlugin, ok := plugin.(*StaticAuthPlugin)
	require.True(t, ok)
	return staticPlugin
}

// authenticate runs plugin.Authenticate with the given credentials passed as
// gRPC metadata, mirroring how the auth interceptors invoke it.
func authenticate(t *testing.T, plugin *StaticAuthPlugin, username, password string) (context.Context, error) {
	t.Helper()

	md := metadata.New(map[string]string{
		"username": username,
		"password": password,
	})
	ctx := metadata.NewIncomingContext(t.Context(), md)
	return plugin.Authenticate(ctx, "/test.Service/Method")
}

// TestStaticAuthPlugin_Authenticate tests authentication against a credentials
// file mixing plaintext and SHA256-hashed passwords.
func TestStaticAuthPlugin_Authenticate(t *testing.T) {
	// SHA256(SHA256("password")) is "73641c99f7719f57d8f4beb11a303afcd190243a51ced8782ca6d3dbe014d146"
	// SHA256(SHA256("secret123")) is "49bbd275dd4bfb1170ced93e839a8ec1d5b86eab6acb0842502130a31702390d"
	plugin := newTestStaticAuthPlugin(t, []StaticAuthConfigEntry{
		{
			Username:            "user1",
			CachingSha2Password: "73641c99f7719f57d8f4beb11a303afcd190243a51ced8782ca6d3dbe014d146", // SHA256(SHA256("password"))
		},
		{
			Username:            "user2",
			CachingSha2Password: "49bbd275dd4bfb1170ced93e839a8ec1d5b86eab6acb0842502130a31702390d", // SHA256(SHA256("secret123"))
		},
		{
			Username: "user3",
			Password: "plaintext_password", // plaintext password
		},
	})

	tests := []struct {
		name           string
		username       string
		password       string
		expectError    bool
		expectedCode   codes.Code
		expectedErrMsg string
	}{
		{
			name:        "valid credentials - user1 (SHA256)",
			username:    "user1",
			password:    "password",
			expectError: false,
		},
		{
			name:        "valid credentials - user2 (SHA256)",
			username:    "user2",
			password:    "secret123",
			expectError: false,
		},
		{
			name:        "valid credentials - user3 (plaintext)",
			username:    "user3",
			password:    "plaintext_password",
			expectError: false,
		},
		{
			name:           "invalid password - SHA256 user",
			username:       "user1",
			password:       "wrongpassword",
			expectError:    true,
			expectedCode:   codes.PermissionDenied,
			expectedErrMsg: "auth failure: caller \"user1\" provided invalid credentials",
		},
		{
			name:           "invalid password - plaintext user",
			username:       "user3",
			password:       "wrongpassword",
			expectError:    true,
			expectedCode:   codes.PermissionDenied,
			expectedErrMsg: "auth failure: caller \"user3\" provided invalid credentials",
		},
		{
			name:           "non-existent user",
			username:       "nonexistent",
			password:       "password",
			expectError:    true,
			expectedCode:   codes.PermissionDenied,
			expectedErrMsg: "auth failure: caller \"nonexistent\" provided invalid credentials",
		},
		{
			name:           "empty password",
			username:       "user1",
			password:       "",
			expectError:    true,
			expectedCode:   codes.PermissionDenied,
			expectedErrMsg: "auth failure: caller \"user1\" provided invalid credentials",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newCtx, err := authenticate(t, plugin, tt.username, tt.password)

			if tt.expectError {
				require.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok, "error should be a gRPC status error")
				require.Equal(t, tt.expectedCode, st.Code())
				require.Contains(t, st.Message(), tt.expectedErrMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, newCtx)

				// Verify username is stored in context
				require.Equal(t, tt.username, StaticAuthUsernameFromContext(newCtx))
			}
		})
	}
}

// TestStaticAuthPlugin_UppercaseHash tests that a CachingSha2Password written
// in uppercase (or mixed-case) hex still authenticates, since the hash is
// hex-decoded at initialization rather than compared as a string.
func TestStaticAuthPlugin_UppercaseHash(t *testing.T) {
	plugin := newTestStaticAuthPlugin(t, []StaticAuthConfigEntry{
		{
			Username:            "user1",
			CachingSha2Password: "73641C99F7719F57D8F4BEB11A303AFCD190243A51CED8782CA6D3DBE014D146", // uppercase SHA256(SHA256("password"))
		},
	})

	newCtx, err := authenticate(t, plugin, "user1", "password")
	require.NoError(t, err)
	require.Equal(t, "user1", StaticAuthUsernameFromContext(newCtx))
}

// TestStaticAuthPlugin_StarPrefixedHash tests that a CachingSha2Password with
// the MySQL-style leading '*' authenticates, so values can be copied verbatim
// from a MySQL static auth configuration.
func TestStaticAuthPlugin_StarPrefixedHash(t *testing.T) {
	plugin := newTestStaticAuthPlugin(t, []StaticAuthConfigEntry{
		{
			Username:            "user1",
			CachingSha2Password: "*73641c99f7719f57d8f4beb11a303afcd190243a51ced8782ca6d3dbe014d146", // SHA256(SHA256("password"))
		},
	})

	newCtx, err := authenticate(t, plugin, "user1", "password")
	require.NoError(t, err)
	require.Equal(t, "user1", StaticAuthUsernameFromContext(newCtx))
}

// TestStaticAuthPlugin_DuplicateUsernameEntries tests that when multiple
// entries share the same username, authentication keeps checking the remaining
// entries after a password mismatch, so any of the configured passwords is
// accepted.
func TestStaticAuthPlugin_DuplicateUsernameEntries(t *testing.T) {
	plugin := newTestStaticAuthPlugin(t, []StaticAuthConfigEntry{
		{
			Username:            "user1",
			CachingSha2Password: "1cb0d8e6f8975f4993ac48974d03a903bba80ed3ea94997ffc4e339c0f1e8b3d", // SHA256(SHA256("first_password"))
		},
		{
			Username:            "user1",
			CachingSha2Password: "064fb3be00bc3bcf9df547e3be390829e45710fca25966fb068798ae4ea5ff76", // SHA256(SHA256("second_password"))
		},
	})

	tests := []struct {
		name        string
		password    string
		expectError bool
	}{
		{
			name:        "first entry matches",
			password:    "first_password",
			expectError: false,
		},
		{
			name:        "first entry fails but second entry matches",
			password:    "second_password",
			expectError: false,
		},
		{
			name:        "no entry matches",
			password:    "wrong_password",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newCtx, err := authenticate(t, plugin, "user1", tt.password)

			if tt.expectError {
				require.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok, "error should be a gRPC status error")
				require.Equal(t, codes.PermissionDenied, st.Code())
			} else {
				require.NoError(t, err)
				require.Equal(t, "user1", StaticAuthUsernameFromContext(newCtx))
			}
		})
	}
}

// TestStaticAuthPlugin_HashTakesPrecedence tests that when an entry has both
// Password and CachingSha2Password set, the SHA256 hash is used and the
// plaintext Password is ignored, matching the MySQL static auth server.
func TestStaticAuthPlugin_HashTakesPrecedence(t *testing.T) {
	plugin := newTestStaticAuthPlugin(t, []StaticAuthConfigEntry{
		{
			Username:            "user1",
			Password:            "plain_password",
			CachingSha2Password: "79cc80902b3f156439204d9c08f77f59a918fbbd302309e0844993d7c21c6da5", // SHA256(SHA256("hashed_password"))
		},
	})

	t.Run("hashed password matches", func(t *testing.T) {
		newCtx, err := authenticate(t, plugin, "user1", "hashed_password")
		require.NoError(t, err)
		require.Equal(t, "user1", StaticAuthUsernameFromContext(newCtx))
	})

	t.Run("plaintext password is ignored", func(t *testing.T) {
		_, err := authenticate(t, plugin, "user1", "plain_password")
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok, "error should be a gRPC status error")
		require.Equal(t, codes.PermissionDenied, st.Code())
	})
}

// TestStaticAuthPlugin_NoMetadata tests that authentication fails with
// Unauthenticated when the incoming context carries no gRPC metadata.
func TestStaticAuthPlugin_NoMetadata(t *testing.T) {
	plugin := newTestStaticAuthPlugin(t, []StaticAuthConfigEntry{
		{
			Username: "user1",
			Password: "password",
		},
	})

	_, err := plugin.Authenticate(t.Context(), "/test.Service/Method")

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unauthenticated, st.Code())
	require.Contains(t, st.Message(), "username and password must be provided")
}

// TestStaticAuthPlugin_MissingCredentials tests that authentication fails with
// Unauthenticated when the username or password metadata keys are absent.
func TestStaticAuthPlugin_MissingCredentials(t *testing.T) {
	plugin := newTestStaticAuthPlugin(t, []StaticAuthConfigEntry{
		{
			Username: "user1",
			Password: "password",
		},
	})

	tests := []struct {
		name     string
		metadata map[string]string
	}{
		{
			name: "missing username",
			metadata: map[string]string{
				"password": "password",
			},
		},
		{
			name: "missing password",
			metadata: map[string]string{
				"username": "user1",
			},
		},
		{
			name:     "missing both",
			metadata: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := metadata.New(tt.metadata)
			ctx := metadata.NewIncomingContext(t.Context(), md)

			_, err := plugin.Authenticate(ctx, "/test.Service/Method")

			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.Unauthenticated, st.Code())
			require.Contains(t, st.Message(), "username and password must be provided")
		})
	}
}

// TestStaticAuthPluginInitializer tests the initialization and validation of
// the static auth plugin credentials file, in particular the SHA256 hash
// validation added for CachingSha2Password entries.
func TestStaticAuthPluginInitializer(t *testing.T) {
	writeConfig := func(t *testing.T, contents string) {
		t.Helper()
		tmpFile, err := os.CreateTemp(t.TempDir(), "static_auth_test_*.json")
		require.NoError(t, err)
		_, err = tmpFile.WriteString(contents)
		require.NoError(t, err)
		require.NoError(t, tmpFile.Close())

		credsFile = tmpFile.Name()
		t.Cleanup(func() { credsFile = "" })
	}

	marshal := func(t *testing.T, entries []StaticAuthConfigEntry) string {
		t.Helper()
		data, err := json.Marshal(entries)
		require.NoError(t, err)
		return string(data)
	}

	t.Run("valid config file", func(t *testing.T) {
		writeConfig(t, marshal(t, []StaticAuthConfigEntry{
			{
				Username:            "testuser",
				CachingSha2Password: "6cbfe567592500d58fdcc0e0bbeca784fbc53bd6159869df6cbeac0b6604d4e9", // SHA256(SHA256("testpass"))
			},
			{
				Username: "plainuser",
				Password: "plainpass",
			},
		}))

		plugin, err := staticAuthPluginInitializer()
		require.NoError(t, err)

		staticPlugin, ok := plugin.(*StaticAuthPlugin)
		require.True(t, ok)
		require.Len(t, staticPlugin.entries, 2)
		require.Equal(t, "testuser", staticPlugin.entries[0].Username)
		require.Len(t, staticPlugin.entries[0].cachingSha2Password, 32)
		require.Equal(t, "plainuser", staticPlugin.entries[1].Username)
		require.Empty(t, staticPlugin.entries[1].cachingSha2Password)
	})

	t.Run("missing file path", func(t *testing.T) {
		credsFile = ""
		t.Cleanup(func() { credsFile = "" })

		_, err := staticAuthPluginInitializer()
		require.Error(t, err)
		require.Contains(t, err.Error(), "grpc-auth-static-password-file not provided")
	})

	t.Run("non-existent file", func(t *testing.T) {
		credsFile = "/nonexistent/path/to/file.json"
		t.Cleanup(func() { credsFile = "" })

		_, err := staticAuthPluginInitializer()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to load static auth plugin")
	})

	t.Run("invalid json", func(t *testing.T) {
		writeConfig(t, "{invalid json")

		_, err := staticAuthPluginInitializer()
		require.Error(t, err)
		require.Contains(t, err.Error(), "fail to load static auth plugin")
	})

	t.Run("invalid hash length", func(t *testing.T) {
		writeConfig(t, marshal(t, []StaticAuthConfigEntry{
			{
				Username:            "testuser",
				CachingSha2Password: "abcdef", // valid hex but not a SHA256 digest
			},
		}))

		_, err := staticAuthPluginInitializer()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid CachingSha2Password length")
	})

	t.Run("invalid hex encoding", func(t *testing.T) {
		writeConfig(t, marshal(t, []StaticAuthConfigEntry{
			{
				Username:            "testuser",
				CachingSha2Password: strings.Repeat("z", 64), // 64 chars but invalid hex
			},
		}))

		_, err := staticAuthPluginInitializer()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid hex-encoded CachingSha2Password")
	})
}
