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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMainFlagRegistration(t *testing.T) {
	registerFlags()

	// Test that the TLS flags are properly registered
	t.Run("grpc tls flags are registered", func(t *testing.T) {
		certFlag := rootCmd.Flags().Lookup("vtctld-grpc-cert")
		require.NotNil(t, certFlag, "vtctld-grpc-cert flag should be registered")
		assert.Equal(t, "", certFlag.DefValue, "vtctld-grpc-cert should have empty default value")
		assert.Equal(t, "the cert to use to connect", certFlag.Usage, "vtctld-grpc-cert should have correct usage")

		keyFlag := rootCmd.Flags().Lookup("vtctld-grpc-key")
		require.NotNil(t, keyFlag, "vtctld-grpc-key flag should be registered")
		assert.Equal(t, "", keyFlag.DefValue, "vtctld-grpc-key should have empty default value")

		caFlag := rootCmd.Flags().Lookup("vtctld-grpc-ca")
		require.NotNil(t, caFlag, "vtctld-grpc-ca flag should be registered")
		assert.Equal(t, "", caFlag.DefValue, "vtctld-grpc-ca should have empty default value")

		crlFlag := rootCmd.Flags().Lookup("vtctld-grpc-crl")
		require.NotNil(t, crlFlag, "vtctld-grpc-crl flag should be registered")
		assert.Equal(t, "", crlFlag.DefValue, "vtctld-grpc-crl should have empty default value")

		serverNameFlag := rootCmd.Flags().Lookup("vtctld-grpc-server-name")
		require.NotNil(t, serverNameFlag, "vtctld-grpc-server-name flag should be registered")
		assert.Equal(t, "", serverNameFlag.DefValue, "vtctld-grpc-server-name should have empty default value")
	})
}
