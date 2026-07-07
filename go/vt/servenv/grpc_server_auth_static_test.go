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

	"github.com/stretchr/testify/assert"
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
	// SHA256 hash of "password" is "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
	// SHA256 hash of "secret123" is "fcf730b6d95236ecd3c9fc2d92d7b6b2bb061514961aec041d6c7a7192f592e4"
	plugin := newTestStaticAuthPlugin(t, []StaticAuthConfigEntry{
		{
			Username:             "user1",
			SHA256HashedPassword: "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8", // "password" as SHA256 hash
		},
		{
			Username:             "user2",
			SHA256HashedPassword: "fcf730b6d95236ecd3c9fc2d92d7b6b2bb061514961aec041d6c7a7192f592e4", // "secret123" as SHA256 hash
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
				assert.Equal(t, tt.expectedCode, st.Code())
				assert.Contains(t, st.Message(), tt.expectedErrMsg)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, newCtx)

				// Verify username is stored in context
				assert.Equal(t, tt.username, StaticAuthUsernameFromContext(newCtx))
			}
		})
	}
}

// TestStaticAuthPlugin_UppercaseHash tests that a SHA256HashedPassword written
// in uppercase (or mixed-case) hex still authenticates, since the hash is
// hex-decoded at initialization rather than compared as a string.
func TestStaticAuthPlugin_UppercaseHash(t *testing.T) {
	plugin := newTestStaticAuthPlugin(t, []StaticAuthConfigEntry{
		{
			Username:             "user1",
			SHA256HashedPassword: "5E884898DA28047151D0E56F8DC6292773603D0D6AABBDD62A11EF721D1542D8", // "password" as uppercase SHA256 hash
		},
	})

	newCtx, err := authenticate(t, plugin, "user1", "password")
	require.NoError(t, err)
	assert.Equal(t, "user1", StaticAuthUsernameFromContext(newCtx))
}

// TestStaticAuthPlugin_DuplicateUsernameEntries tests that when multiple
// entries share the same username, authentication keeps checking the remaining
// entries after a password mismatch, so any of the configured passwords is
// accepted.
func TestStaticAuthPlugin_DuplicateUsernameEntries(t *testing.T) {
	plugin := newTestStaticAuthPlugin(t, []StaticAuthConfigEntry{
		{
			Username:             "user1",
			SHA256HashedPassword: "faa9ef1976a332ec21a17d4a88fb0cae8ce93743b3f8c69370161d6b38839898", // "first_password" as SHA256 hash
		},
		{
			Username:             "user1",
			SHA256HashedPassword: "900cc4a6adbb527ac2ef8ac8f4d92104a040befa1c46c21bb6bab09eb72fc565", // "second_password" as SHA256 hash
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
				assert.Equal(t, codes.PermissionDenied, st.Code())
			} else {
				require.NoError(t, err)
				assert.Equal(t, "user1", StaticAuthUsernameFromContext(newCtx))
			}
		})
	}
}

// TestStaticAuthPlugin_PlaintextTakesPrecedence tests that when an entry has
// both Password and SHA256HashedPassword set, the plaintext Password is used
// and the SHA256 hash is ignored.
func TestStaticAuthPlugin_PlaintextTakesPrecedence(t *testing.T) {
	plugin := newTestStaticAuthPlugin(t, []StaticAuthConfigEntry{
		{
			Username:             "user1",
			Password:             "plain_password",
			SHA256HashedPassword: "b2867617492e26c338ab49f72afabc984d798b59755a27e312b953716ae964d7", // "hashed_password" as SHA256 hash
		},
	})

	t.Run("plaintext password matches", func(t *testing.T) {
		newCtx, err := authenticate(t, plugin, "user1", "plain_password")
		require.NoError(t, err)
		assert.Equal(t, "user1", StaticAuthUsernameFromContext(newCtx))
	})

	t.Run("hashed password is ignored", func(t *testing.T) {
		_, err := authenticate(t, plugin, "user1", "hashed_password")
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok, "error should be a gRPC status error")
		assert.Equal(t, codes.PermissionDenied, st.Code())
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
	assert.Equal(t, codes.Unauthenticated, st.Code())
	assert.Contains(t, st.Message(), "username and password must be provided")
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
			assert.Equal(t, codes.Unauthenticated, st.Code())
			assert.Contains(t, st.Message(), "username and password must be provided")
		})
	}
}

// TestStaticAuthPluginInitializer tests the initialization and validation of
// the static auth plugin credentials file, in particular the SHA256 hash
// validation added for SHA256HashedPassword entries.
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
				Username:             "testuser",
				SHA256HashedPassword: "13d249f2cb4127b40cfa757866850278793f814ded3c587fe5889e889a7a9f6c", // "testpass" as SHA256 hash
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
		assert.Equal(t, "testuser", staticPlugin.entries[0].Username)
		assert.Len(t, staticPlugin.entries[0].sha256HashedPassword, 32)
		assert.Equal(t, "plainuser", staticPlugin.entries[1].Username)
		assert.Empty(t, staticPlugin.entries[1].sha256HashedPassword)
	})

	t.Run("missing file path", func(t *testing.T) {
		credsFile = ""
		t.Cleanup(func() { credsFile = "" })

		_, err := staticAuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "grpc-auth-static-password-file not provided")
	})

	t.Run("non-existent file", func(t *testing.T) {
		credsFile = "/nonexistent/path/to/file.json"
		t.Cleanup(func() { credsFile = "" })

		_, err := staticAuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load static auth plugin")
	})

	t.Run("invalid json", func(t *testing.T) {
		writeConfig(t, "{invalid json")

		_, err := staticAuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fail to load static auth plugin")
	})

	t.Run("invalid hash length", func(t *testing.T) {
		writeConfig(t, marshal(t, []StaticAuthConfigEntry{
			{
				Username:             "testuser",
				SHA256HashedPassword: "abcdef", // valid hex but not a SHA256 digest
			},
		}))

		_, err := staticAuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid SHA256HashedPassword length")
	})

	t.Run("invalid hex encoding", func(t *testing.T) {
		writeConfig(t, marshal(t, []StaticAuthConfigEntry{
			{
				Username:             "testuser",
				SHA256HashedPassword: strings.Repeat("z", 64), // 64 chars but invalid hex
			},
		}))

		_, err := staticAuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid hex-encoded SHA256HashedPassword")
	})
}
