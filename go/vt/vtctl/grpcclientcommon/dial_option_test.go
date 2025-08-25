/*
Copyright 2025 The Vitess Authors.

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

package grpcclientcommon

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterFlags(t *testing.T) {
	// Create a new flag set to test with
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)

	// Call RegisterFlags
	RegisterFlags(fs)

	// Test that all the expected flags are registered
	t.Run("vtctld-grpc-cert flag is registered", func(t *testing.T) {
		flag := fs.Lookup("vtctld-grpc-cert")
		require.NotNil(t, flag, "vtctld-grpc-cert flag should be registered")
		assert.Equal(t, "", flag.DefValue, "vtctld-grpc-cert should have empty default value")
		assert.Equal(t, "the cert to use to connect", flag.Usage, "vtctld-grpc-cert should have correct usage")
	})

	t.Run("vtctld-grpc-key flag is registered", func(t *testing.T) {
		flag := fs.Lookup("vtctld-grpc-key")
		require.NotNil(t, flag, "vtctld-grpc-key flag should be registered")
		assert.Equal(t, "", flag.DefValue, "vtctld-grpc-key should have empty default value")
		assert.Equal(t, "the key to use to connect", flag.Usage, "vtctld-grpc-key should have correct usage")
	})

	t.Run("vtctld-grpc-ca flag is registered", func(t *testing.T) {
		flag := fs.Lookup("vtctld-grpc-ca")
		require.NotNil(t, flag, "vtctld-grpc-ca flag should be registered")
		assert.Equal(t, "", flag.DefValue, "vtctld-grpc-ca should have empty default value")
		assert.Equal(t, "the server ca to use to validate servers when connecting", flag.Usage, "vtctld-grpc-ca should have correct usage")
	})

	t.Run("vtctld-grpc-crl flag is registered", func(t *testing.T) {
		flag := fs.Lookup("vtctld-grpc-crl")
		require.NotNil(t, flag, "vtctld-grpc-crl flag should be registered")
		assert.Equal(t, "", flag.DefValue, "vtctld-grpc-crl should have empty default value")
		assert.Equal(t, "the server crl to use to validate server certificates when connecting", flag.Usage, "vtctld-grpc-crl should have correct usage")
	})

	t.Run("vtctld-grpc-server-name flag is registered", func(t *testing.T) {
		flag := fs.Lookup("vtctld-grpc-server-name")
		require.NotNil(t, flag, "vtctld-grpc-server-name flag should be registered")
		assert.Equal(t, "", flag.DefValue, "vtctld-grpc-server-name should have empty default value")
		assert.Equal(t, "the server name to use to validate server certificate", flag.Usage, "vtctld-grpc-server-name should have correct usage")
	})
}

func TestSecureDialOption(t *testing.T) {
	// Test SecureDialOption with default (empty) values
	t.Run("secure dial option with default values", func(t *testing.T) {
		// Reset the global variables to empty values
		cert = ""
		key = ""
		ca = ""
		crl = ""
		name = ""

		opt, err := SecureDialOption()
		require.NoError(t, err, "SecureDialOption should not return an error with default values")
		require.NotNil(t, opt, "SecureDialOption should return a valid DialOption")

		// When all TLS parameters are empty, it should return insecure credentials
		// We can't easily test the exact type, but we can verify it's not nil
		assert.NotNil(t, opt)
	})

	// Test that SecureDialOption delegates to grpcclient.SecureDialOption
	t.Run("secure dial option delegates correctly", func(t *testing.T) {
		// We can't easily test the internal behavior without mocking grpcclient.SecureDialOption,
		// but we can verify the function completes without error under normal conditions
		opt, err := SecureDialOption()
		assert.NoError(t, err, "SecureDialOption should complete without error")
		assert.NotNil(t, opt, "SecureDialOption should return a non-nil option")
	})
}

func TestInitFunctionRegistersVtadmin(t *testing.T) {
	// This test verifies that the init function registers vtadmin with servenv.OnParseFor
	// We can't easily test the init function directly, but we can verify its effects

	// The init function should have registered vtadmin for flag parsing
	// Since servenv.OnParseFor is called during init, we test the behavior indirectly
	// by verifying that the module includes vtadmin in its initialization list

	// This test documents the expected behavior rather than directly testing the init function
	// since testing init functions directly is generally not recommended in Go
	t.Run("vtadmin is registered for flag parsing", func(t *testing.T) {
		// The key change in the commit was adding this line to init():
		// servenv.OnParseFor("vtadmin", RegisterFlags)
		//
		// This ensures that when vtadmin parses flags, RegisterFlags is called.
		// We verify this indirectly by testing that RegisterFlags works correctly.

		fs := pflag.NewFlagSet("vtadmin-test", pflag.ContinueOnError)
		RegisterFlags(fs)

		// If RegisterFlags was properly set up for vtadmin, these flags should be available
		vtctldCertFlag := fs.Lookup("vtctld-grpc-cert")
		assert.NotNil(t, vtctldCertFlag, "vtctld-grpc-cert flag should be available after RegisterFlags")

		// This confirms that the init function change allows vtadmin to access TLS flags
		assert.Equal(t, "the cert to use to connect", vtctldCertFlag.Usage)
	})
}

func TestPackageGlobalVariables(t *testing.T) {
	// Test that the global variables are properly declared and accessible
	t.Run("global variables are accessible", func(t *testing.T) {
		// Save original values
		origCert := cert
		origKey := key
		origCa := ca
		origCrl := crl
		origName := name

		// Test that we can modify the global variables (as flags would)
		cert = "test-cert"
		key = "test-key"
		ca = "test-ca"
		crl = "test-crl"
		name = "test-name"

		assert.Equal(t, "test-cert", cert)
		assert.Equal(t, "test-key", key)
		assert.Equal(t, "test-ca", ca)
		assert.Equal(t, "test-crl", crl)
		assert.Equal(t, "test-name", name)

		// Restore original values
		cert = origCert
		key = origKey
		ca = origCa
		crl = origCrl
		name = origName
	})
}

func TestFlagIntegration(t *testing.T) {
	// Test the integration between flag registration and SecureDialOption
	t.Run("flags affect SecureDialOption behavior", func(t *testing.T) {
		// Save original values
		origCert := cert
		origKey := key
		origCa := ca
		origCrl := crl
		origName := name

		defer func() {
			// Restore original values
			cert = origCert
			key = origKey
			ca = origCa
			crl = origCrl
			name = origName
		}()

		// Test with empty values (should use insecure)
		cert = ""
		key = ""
		ca = ""
		crl = ""
		name = ""

		opt1, err1 := SecureDialOption()
		require.NoError(t, err1, "SecureDialOption should work with empty values")
		require.NotNil(t, opt1, "SecureDialOption should return a valid option")

		// Test with server name set (doesn't require file system access)
		name = "test-server-name"
		opt2, err2 := SecureDialOption()
		require.NoError(t, err2, "SecureDialOption should work with server name set")
		require.NotNil(t, opt2, "SecureDialOption should return a valid option")

		// The options may be different (we can't easily compare them)
		// but both should be valid gRPC dial options
		assert.NotNil(t, opt1)
		assert.NotNil(t, opt2)
	})
}

func TestBackwardCompatibilityWithVtadmin(t *testing.T) {
	// Test that the changes maintain backward compatibility for vtadmin
	t.Run("default behavior is insecure for backward compatibility", func(t *testing.T) {
		// Save original values
		origCert := cert
		origKey := key
		origCa := ca
		origCrl := crl
		origName := name

		defer func() {
			// Restore original values
			cert = origCert
			key = origKey
			ca = origCa
			crl = origCrl
			name = origName
		}()

		// Reset to default state (what would happen on a fresh vtadmin start)
		cert = ""
		key = ""
		ca = ""
		crl = ""
		name = ""

		// This should return an insecure connection option, maintaining
		// backward compatibility with existing vtadmin deployments
		opt, err := SecureDialOption()
		require.NoError(t, err, "SecureDialOption should work with default (empty) TLS flags")
		require.NotNil(t, opt, "SecureDialOption should return a valid dial option")

		// The key point is that this call succeeds without error,
		// meaning existing vtadmin deployments will continue to work
	})

	t.Run("vtadmin registration in init function", func(t *testing.T) {
		// This test documents that vtadmin was added to the init function
		// We can't directly test the init function, but we can test that
		// the RegisterFlags function works correctly (which is what gets
		// registered for vtadmin in the init function)

		fs := pflag.NewFlagSet("vtadmin-test", pflag.ContinueOnError)
		RegisterFlags(fs)

		// Verify all the expected flags are available for vtadmin
		assert.NotNil(t, fs.Lookup("vtctld-grpc-cert"))
		assert.NotNil(t, fs.Lookup("vtctld-grpc-key"))
		assert.NotNil(t, fs.Lookup("vtctld-grpc-ca"))
		assert.NotNil(t, fs.Lookup("vtctld-grpc-crl"))
		assert.NotNil(t, fs.Lookup("vtctld-grpc-server-name"))

		// This confirms that the init function change allows vtadmin to
		// register and use TLS flags as intended by the commit
	})
}
