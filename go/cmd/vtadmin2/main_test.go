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
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtadmin/vtadmin2"
)

func TestBuildVTAdmin2OptionsDefaultsTitle(t *testing.T) {
	uiOpts = vtadmin2.Options{}

	opts := buildVTAdmin2Options()

	assert.Equal(t, "VTAdmin2", opts.DocumentTitle)
}

func TestValidateRBACFlagsRequiresExplicitChoice(t *testing.T) {
	enableRBAC = false
	disableRBAC = false

	err := validateRBACFlags()

	require.ErrorContains(t, err, "must explicitly enable or disable RBAC")
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

func TestNormalizeListenAndServeErrorIgnoresClosedServer(t *testing.T) {
	err := normalizeListenAndServeError(http.ErrServerClosed)

	assert.NoError(t, err)
}

func TestNormalizeListenAndServeErrorReturnsOtherErrors(t *testing.T) {
	listenErr := errors.New("listen failed")
	err := normalizeListenAndServeError(listenErr)

	assert.ErrorIs(t, err, listenErr)
}
