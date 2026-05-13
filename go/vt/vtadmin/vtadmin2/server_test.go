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

package vtadmin2

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

type fakeVTAdminServer struct {
	vtadminpb.UnimplementedVTAdminServer
}

func TestNewServerRequiresAPI(t *testing.T) {
	s, err := NewServer(nil, Options{})

	require.ErrorContains(t, err, "requires a VTAdmin server")
	assert.Nil(t, s)
}

func TestNewServerRegistersStaticAssets(t *testing.T) {
	s, err := NewServer(&fakeVTAdminServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/static/vtadmin2.css", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "--vt-color-primary")
}

func TestRootRedirectsToClusters(t *testing.T) {
	s, err := NewServer(&fakeVTAdminServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Equal(t, "/clusters", rec.Header().Get("Location"))
}
