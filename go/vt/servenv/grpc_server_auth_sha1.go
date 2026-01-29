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
	"crypto/sha1"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Sha1AuthPlugin implements the Authenticator interface using SHA1 password hashes.
// It reads credentials from a JSON file where passwords are stored as SHA1 hashes.
type Sha1AuthPlugin struct {
	entries []Sha1AuthConfigEntry
}

// Sha1AuthConfigEntry stores username with either plaintext password or SHA1-hashed password.
// If Password is set, plaintext authentication is used.
// If SHA1HashedPassword is set (and Password is empty), SHA1 hash authentication is used.
// This allows mixing both authentication methods in the same configuration file.
type Sha1AuthConfigEntry struct {
	Username           string
	Password           string // Plaintext password (optional) - takes precedence if set
	SHA1HashedPassword string // SHA1 hash in hex format (optional) - used if Password is empty
}

// The datatype for sha1 auth Context keys
type sha1AuthKey int

const (
	// Internal Context key for the authenticated username
	sha1AuthUsername sha1AuthKey = 0
)

var (
	// grpcAuthSha1PasswordFile is the path to the JSON file containing SHA1 credentials
	grpcAuthSha1PasswordFile string

	_ Authenticator = (*Sha1AuthPlugin)(nil)
)

func init() {
	RegisterAuthPlugin("sha1", sha1AuthPluginInitializer)
	grpcAuthServerFlagHooks = append(grpcAuthServerFlagHooks, registerGRPCServerAuthSha1Flags)
}

// registerGRPCServerAuthSha1Flags registers the flags for SHA1 authentication plugin.
func registerGRPCServerAuthSha1Flags(fs *pflag.FlagSet) {
	fs.StringVar(&grpcAuthSha1PasswordFile, "grpc-auth-sha1-password-file", grpcAuthSha1PasswordFile, "JSON file containing username and SHA1 password hashes for gRPC sha1 auth")
}

// sha1AuthPluginInitializer initializes the SHA1 authentication plugin.
func sha1AuthPluginInitializer() (Authenticator, error) {
	if grpcAuthSha1PasswordFile == "" {
		return nil, fmt.Errorf("grpc-auth-sha1-password-file flag must be provided when using sha1 auth")
	}

	data, err := os.ReadFile(grpcAuthSha1PasswordFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load sha1 auth plugin credentials file: %w", err)
	}

	entries := make([]Sha1AuthConfigEntry, 0)

	err = json.Unmarshal(data, &entries)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sha1 auth plugin credentials file: %w", err)
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("sha1 auth plugin credentials file is empty")
	}

	// Validate entries
	for i, entry := range entries {
		if entry.Username == "" {
			return nil, fmt.Errorf("sha1 auth plugin entry %d has empty username", i)
		}

		// Entry must have either Password or SHA1HashedPassword
		if entry.Password == "" && entry.SHA1HashedPassword == "" {
			return nil, fmt.Errorf("sha1 auth plugin entry %d (user=%s) must have either Password or SHA1HashedPassword", i, entry.Username)
		}

		// If SHA1HashedPassword is set, validate it's a valid SHA1 hash
		if entry.SHA1HashedPassword != "" {
			if len(entry.SHA1HashedPassword) != 40 {
				return nil, fmt.Errorf("sha1 auth plugin entry %d (user=%s) has invalid SHA1 hash length: expected 40 hex chars, got %d", i, entry.Username, len(entry.SHA1HashedPassword))
			}
			if _, err := hex.DecodeString(entry.SHA1HashedPassword); err != nil {
				return nil, fmt.Errorf("sha1 auth plugin entry %d (user=%s) has invalid hex-encoded SHA1 hash: %w", i, entry.Username, err)
			}
		}
	}

	return &Sha1AuthPlugin{
		entries: entries,
	}, nil
}

// Authenticate implements the Authenticator interface for hybrid plaintext/SHA1 authentication.
// It extracts username and password from gRPC metadata and compares them against stored credentials.
// If the entry has a plaintext Password field, it uses plaintext comparison.
// If the entry has a SHA1HashedPassword field, it hashes the incoming password and compares.
func (sa *Sha1AuthPlugin) Authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "username and password must be provided")
	}

	if len(md["username"]) == 0 || len(md["password"]) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "username and password must be provided")
	}

	username := md["username"][0]
	password := md["password"][0]

	// Find matching entry and authenticate
	for _, authEntry := range sa.entries {
		if username == authEntry.Username {
			var authenticated bool

			// Prefer plaintext password if set
			if authEntry.Password != "" {
				// Plaintext password comparison using constant-time comparison
				authenticated = subtle.ConstantTimeCompare([]byte(password), []byte(authEntry.Password)) == 1
			} else if authEntry.SHA1HashedPassword != "" {
				// SHA1 hash comparison
				hasher := sha1.New()
				hasher.Write([]byte(password))
				passwordHash := hasher.Sum(nil)
				passwordHashHex := hex.EncodeToString(passwordHash)
				authenticated = subtle.ConstantTimeCompare([]byte(passwordHashHex), []byte(authEntry.SHA1HashedPassword)) == 1
			}

			if authenticated {
				return newSha1AuthContext(ctx, username), nil
			}
			// Username matched but password didn't - still check remaining entries
			// in case there are multiple entries for the same username
		}
	}

	return nil, status.Errorf(codes.PermissionDenied, "auth failure: caller %q provided invalid credentials", username)
}

// newSha1AuthContext creates a new context with the authenticated username stored in it.
func newSha1AuthContext(ctx context.Context, username string) context.Context {
	return context.WithValue(ctx, sha1AuthUsername, username)
}

// Sha1AuthUsernameFromContext retrieves the authenticated username from the context.
// Returns empty string if not found.
func Sha1AuthUsernameFromContext(ctx context.Context) string {
	username, ok := ctx.Value(sha1AuthUsername).(string)
	if ok {
		return username
	}
	return ""
}
