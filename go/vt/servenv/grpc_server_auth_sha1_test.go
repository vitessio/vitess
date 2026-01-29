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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestSha1AuthPlugin(t *testing.T) {
	// SHA1 hash of "password" is "5baa61e4c9b93f3f0682250b6cf8331b7ee68fd8"
	// SHA1 hash of "secret123" is "f2b14f68eb995facb3a1c35287b778d5bd785511"
	entries := []Sha1AuthConfigEntry{
		{
			Username:           "user1",
			SHA1HashedPassword: "5baa61e4c9b93f3f0682250b6cf8331b7ee68fd8", // "password" as SHA1 hash
		},
		{
			Username:           "user2",
			SHA1HashedPassword: "f2b14f68eb995facb3a1c35287b778d5bd785511", // "secret123" as SHA1 hash
		},
		{
			Username: "user3",
			Password: "plaintext_password", // plaintext password
		},
	}

	plugin := &Sha1AuthPlugin{
		entries: entries,
	}

	tests := []struct {
		name           string
		username       string
		password       string
		expectError    bool
		expectedCode   codes.Code
		expectedErrMsg string
	}{
		{
			name:        "valid credentials - user1 (SHA1)",
			username:    "user1",
			password:    "password",
			expectError: false,
		},
		{
			name:        "valid credentials - user2 (SHA1)",
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
			name:           "invalid password - SHA1 user",
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
			md := metadata.New(map[string]string{
				"username": tt.username,
				"password": tt.password,
			})
			ctx := metadata.NewIncomingContext(context.Background(), md)

			newCtx, err := plugin.Authenticate(ctx, "/test.Service/Method")

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
				username := Sha1AuthUsernameFromContext(newCtx)
				assert.Equal(t, tt.username, username)
			}
		})
	}
}

func TestSha1AuthPlugin_NoMetadata(t *testing.T) {
	plugin := &Sha1AuthPlugin{
		entries: []Sha1AuthConfigEntry{
			{
				Username:           "user1",
				SHA1HashedPassword: "5baa61e4c9b93f3f0682250b6cf8331b7ee68fd8",
			},
		},
	}

	ctx := context.Background()
	_, err := plugin.Authenticate(ctx, "/test.Service/Method")

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())
	assert.Contains(t, st.Message(), "username and password must be provided")
}

func TestSha1AuthPlugin_MissingCredentials(t *testing.T) {
	plugin := &Sha1AuthPlugin{
		entries: []Sha1AuthConfigEntry{
			{
				Username:           "user1",
				SHA1HashedPassword: "5baa61e4c9b93f3f0682250b6cf8331b7ee68fd8",
			},
		},
	}

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
			ctx := metadata.NewIncomingContext(context.Background(), md)

			_, err := plugin.Authenticate(ctx, "/test.Service/Method")

			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.Unauthenticated, st.Code())
			assert.Contains(t, st.Message(), "username and password must be provided")
		})
	}
}

func TestSha1AuthPluginInitializer(t *testing.T) {
	// SHA1 hash of "testpass" is "5be5e59bfe3a8a8063fcfbfc70d0cc8b9cc06e77"
	validConfig := []Sha1AuthConfigEntry{
		{
			Username:           "testuser",
			SHA1HashedPassword: "5be5e59bfe3a8a8063fcfbfc70d0cc8b9cc06e77",
		},
	}

	t.Run("valid config file", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "sha1_auth_test_*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		configData, err := json.Marshal(validConfig)
		require.NoError(t, err)
		_, err = tmpFile.Write(configData)
		require.NoError(t, err)
		tmpFile.Close()

		grpcAuthSha1PasswordFile = tmpFile.Name()
		defer func() { grpcAuthSha1PasswordFile = "" }()

		plugin, err := sha1AuthPluginInitializer()
		require.NoError(t, err)
		assert.NotNil(t, plugin)

		sha1Plugin, ok := plugin.(*Sha1AuthPlugin)
		require.True(t, ok)
		assert.Len(t, sha1Plugin.entries, 1)
		assert.Equal(t, "testuser", sha1Plugin.entries[0].Username)
	})

	t.Run("missing file path", func(t *testing.T) {
		grpcAuthSha1PasswordFile = ""
		defer func() { grpcAuthSha1PasswordFile = "" }()

		_, err := sha1AuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "grpc-auth-sha1-password-file flag must be provided")
	})

	t.Run("non-existent file", func(t *testing.T) {
		grpcAuthSha1PasswordFile = "/nonexistent/path/to/file.json"
		defer func() { grpcAuthSha1PasswordFile = "" }()

		_, err := sha1AuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load sha1 auth plugin credentials file")
	})

	t.Run("invalid json", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "sha1_auth_test_*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString("{invalid json")
		require.NoError(t, err)
		tmpFile.Close()

		grpcAuthSha1PasswordFile = tmpFile.Name()
		defer func() { grpcAuthSha1PasswordFile = "" }()

		_, err = sha1AuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse sha1 auth plugin credentials file")
	})

	t.Run("empty config file", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "sha1_auth_test_*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString("[]")
		require.NoError(t, err)
		tmpFile.Close()

		grpcAuthSha1PasswordFile = tmpFile.Name()
		defer func() { grpcAuthSha1PasswordFile = "" }()

		_, err = sha1AuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sha1 auth plugin credentials file is empty")
	})

	t.Run("empty username", func(t *testing.T) {
		invalidConfig := []Sha1AuthConfigEntry{
			{
				Username: "",
				Password: "5be5e59bfe3a8a8063fcfbfc70d0cc8b9cc06e77",
			},
		}

		tmpFile, err := os.CreateTemp("", "sha1_auth_test_*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		configData, err := json.Marshal(invalidConfig)
		require.NoError(t, err)
		_, err = tmpFile.Write(configData)
		require.NoError(t, err)
		tmpFile.Close()

		grpcAuthSha1PasswordFile = tmpFile.Name()
		defer func() { grpcAuthSha1PasswordFile = "" }()

		_, err = sha1AuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "has empty username")
	})

	t.Run("missing both password fields", func(t *testing.T) {
		invalidConfig := []Sha1AuthConfigEntry{
			{
				Username: "testuser",
				// No Password or SHA1HashedPassword
			},
		}

		tmpFile, err := os.CreateTemp("", "sha1_auth_test_*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		configData, err := json.Marshal(invalidConfig)
		require.NoError(t, err)
		_, err = tmpFile.Write(configData)
		require.NoError(t, err)
		tmpFile.Close()

		grpcAuthSha1PasswordFile = tmpFile.Name()
		defer func() { grpcAuthSha1PasswordFile = "" }()

		_, err = sha1AuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must have either Password or SHA1HashedPassword")
	})

	t.Run("invalid hash length", func(t *testing.T) {
		invalidConfig := []Sha1AuthConfigEntry{
			{
				Username:           "testuser",
				SHA1HashedPassword: "tooshort",
			},
		}

		tmpFile, err := os.CreateTemp("", "sha1_auth_test_*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		configData, err := json.Marshal(invalidConfig)
		require.NoError(t, err)
		_, err = tmpFile.Write(configData)
		require.NoError(t, err)
		tmpFile.Close()

		grpcAuthSha1PasswordFile = tmpFile.Name()
		defer func() { grpcAuthSha1PasswordFile = "" }()

		_, err = sha1AuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "has invalid SHA1 hash length")
	})

	t.Run("invalid hex encoding", func(t *testing.T) {
		invalidConfig := []Sha1AuthConfigEntry{
			{
				Username:           "testuser",
				SHA1HashedPassword: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", // 40 chars but invalid hex
			},
		}

		tmpFile, err := os.CreateTemp("", "sha1_auth_test_*.json")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		configData, err := json.Marshal(invalidConfig)
		require.NoError(t, err)
		_, err = tmpFile.Write(configData)
		require.NoError(t, err)
		tmpFile.Close()

		grpcAuthSha1PasswordFile = tmpFile.Name()
		defer func() { grpcAuthSha1PasswordFile = "" }()

		_, err = sha1AuthPluginInitializer()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "has invalid hex-encoded SHA1 hash")
	})
}

func TestSha1AuthUsernameFromContext(t *testing.T) {
	t.Run("username present", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), sha1AuthUsername, "testuser")
		username := Sha1AuthUsernameFromContext(ctx)
		assert.Equal(t, "testuser", username)
	})

	t.Run("username not present", func(t *testing.T) {
		ctx := context.Background()
		username := Sha1AuthUsernameFromContext(ctx)
		assert.Equal(t, "", username)
	})

	t.Run("wrong type in context", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), sha1AuthUsername, 12345)
		username := Sha1AuthUsernameFromContext(ctx)
		assert.Equal(t, "", username)
	})
}
