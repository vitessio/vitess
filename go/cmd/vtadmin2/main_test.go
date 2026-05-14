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

package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtadmin/vtadmin2"
)

func TestBuildVTAdmin2OptionsDefaultsTitle(t *testing.T) {
	uiOpts = vtadmin2.Options{}

	opts := buildVTAdmin2Options(nil)

	assert.Equal(t, "VTAdmin2", opts.DocumentTitle)
}

func TestValidateRBACFlagsRequiresExplicitChoice(t *testing.T) {
	enableRBAC = false
	disableRBAC = false

	err := validateRBACFlags()

	require.ErrorContains(t, err, "must explicitly enable or disable RBAC")
}

func TestValidateFlagsRejectsDynamicClusters(t *testing.T) {
	enableRBAC = false
	disableRBAC = true
	enableDynamicClusters = true
	t.Cleanup(func() { enableDynamicClusters = false })

	err := validateFlags()

	require.ErrorContains(t, err, "vtadmin2 does not support dynamic clusters")
}

func TestBuildRuntimeConfigLoadsRBACOnceForAPIAndUI(t *testing.T) {
	rbacPath := filepath.Join(t.TempDir(), "rbac.yaml")
	require.NoError(t, os.WriteFile(rbacPath, []byte(`rules:
  - resource: "*"
    actions: ["*"]
    subjects: ["*"]
    clusters: ["*"]
`), 0o600))

	enableRBAC = true
	disableRBAC = false
	rbacConfigPath = rbacPath
	uiOpts = vtadmin2.Options{}
	t.Cleanup(func() {
		enableRBAC = false
		disableRBAC = false
		rbacConfigPath = ""
	})

	cfg, err := buildRuntimeConfig()

	require.NoError(t, err)
	require.NotNil(t, cfg.rbac)
	assert.Equal(t, cfg.rbac.GetAuthenticator(), cfg.ui.Authenticator)
}

func TestBuildRuntimeConfigFailsWhenRBACCannotLoad(t *testing.T) {
	enableRBAC = true
	disableRBAC = false
	rbacConfigPath = filepath.Join(t.TempDir(), "missing.yaml")
	t.Cleanup(func() {
		enableRBAC = false
		disableRBAC = false
		rbacConfigPath = ""
	})

	cfg, err := buildRuntimeConfig()

	require.Error(t, err)
	assert.Nil(t, cfg)
}

func TestBuildHTTPServerConfiguresTimeouts(t *testing.T) {
	server := buildHTTPServer("127.0.0.1:0", http.NewServeMux())

	assert.Equal(t, "127.0.0.1:0", server.Addr)
	assert.NotNil(t, server.Handler)
	assert.GreaterOrEqual(t, server.ReadHeaderTimeout, 5*time.Second)
	assert.GreaterOrEqual(t, server.ReadTimeout, 30*time.Second)
	assert.GreaterOrEqual(t, server.WriteTimeout, 30*time.Second)
	assert.GreaterOrEqual(t, server.IdleTimeout, 30*time.Second)
}

func TestServeHTTPServerShutsDownWhenContextIsCanceled(t *testing.T) {
	shutdownCalled := make(chan struct{})
	server := &http.Server{Handler: http.NewServeMux()}
	server.RegisterOnShutdown(func() { close(shutdownCalled) })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- serveHTTPServerWithListener(ctx, server, listener)
	}()

	waitUntilServing(t, "http://"+listener.Addr().String())
	cancel()

	assert.Eventually(t, func() bool {
		select {
		case <-shutdownCalled:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond)
	require.NoError(t, <-errCh)
}

func TestNormalizeListenAndServeErrorIgnoresClosedServer(t *testing.T) {
	err := normalizeListenAndServeError(http.ErrServerClosed)

	assert.NoError(t, err)
}

func TestNormalizeListenAndServeErrorReturnsOtherErrors(t *testing.T) {
	listenErr := errors.New("listen failed")
	err := normalizeListenAndServeError(listenErr)

	assert.ErrorIs(t, err, listenErr)
}

func waitUntilServing(t *testing.T, url string) {
	t.Helper()

	assert.Eventually(t, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return true
	}, 30*time.Second, 10*time.Millisecond)
}
