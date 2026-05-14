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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	"vitess.io/vitess/go/vt/vtadmin/rbac"
)

type fakeVTAdminServer struct {
	vtadminpb.UnimplementedVTAdminServer
}

type fakeAuthenticator struct{}

type rejectingAuthenticator struct {
	called bool
}

func (fakeAuthenticator) Authenticate(ctx context.Context) (*rbac.Actor, error) {
	return &rbac.Actor{Name: "cli", Roles: []string{"admin"}}, nil
}

func (fakeAuthenticator) AuthenticateHTTP(r *http.Request) (*rbac.Actor, error) {
	return &rbac.Actor{Name: "browser", Roles: []string{"admin"}}, nil
}

func (a *rejectingAuthenticator) Authenticate(ctx context.Context) (*rbac.Actor, error) {
	a.called = true
	return nil, errors.New("rejected")
}

func (a *rejectingAuthenticator) AuthenticateHTTP(r *http.Request) (*rbac.Actor, error) {
	a.called = true
	return nil, errors.New("rejected")
}

func TestNewServerRequiresAPI(t *testing.T) {
	s, err := NewServer(nil, Options{})

	require.ErrorContains(t, err, "requires a VTAdmin server")
	assert.Nil(t, s)
}

func TestServerUsesStandardLibraryRouter(t *testing.T) {
	s, err := NewServer(&fakeVTAdminServer{}, Options{})
	require.NoError(t, err)

	assert.IsType(t, http.NewServeMux(), s.router)
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

func TestHeadPageRoutesUseGetHandler(t *testing.T) {
	fake := &pageFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodHead, "/clusters", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, fake.getClustersCalled)
}

func TestUnknownPathReturnsNotFound(t *testing.T) {
	s, err := NewServer(&fakeVTAdminServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestUnknownPathSkipsAuthentication(t *testing.T) {
	authenticator := &rejectingAuthenticator{}
	s, err := NewServer(&fakeVTAdminServer{}, Options{Authenticator: authenticator})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.False(t, authenticator.called)
}

func TestStaticDirectoryWithoutSlashReturnsNotFound(t *testing.T) {
	s, err := NewServer(&fakeVTAdminServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/static", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestStaticAssetsAuthenticateBeforeMethodHandling(t *testing.T) {
	authenticator := &rejectingAuthenticator{}
	s, err := NewServer(&fakeVTAdminServer{}, Options{Authenticator: authenticator})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/static/vtadmin2.css", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.False(t, authenticator.called)
}

func TestStaticAssetsSkipAuthentication(t *testing.T) {
	authenticator := &rejectingAuthenticator{}
	s, err := NewServer(&fakeVTAdminServer{}, Options{Authenticator: authenticator})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/static/vtadmin2.css", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.False(t, authenticator.called)
}

func TestNavigationIncludesReadOnlyParitySections(t *testing.T) {
	s, err := NewServer(&fakeVTAdminServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	s.ServeHTTP(rec, req)

	body := rec.Body.String()
	assert.Contains(t, body, "href=\"/gates\"")
	assert.Contains(t, body, "href=\"/vtctlds\"")
	assert.Contains(t, body, "href=\"/cells\"")
	assert.Contains(t, body, "href=\"/backups\"")
	assert.Contains(t, body, "href=\"/topology\"")
	assert.Contains(t, body, "href=\"/shards\"")
	assert.Contains(t, body, "href=\"/vschemas\"")
	assert.Contains(t, body, "href=\"/srvkeyspaces\"")
	assert.Contains(t, body, "href=\"/srvvschemas\"")
	assert.Contains(t, body, "href=\"/workflows\"")
	assert.Contains(t, body, "href=\"/migrations\"")
	assert.Contains(t, body, "href=\"/transactions\"")
	assert.Contains(t, body, "href=\"/vtexplain\"")
	assert.Contains(t, body, "href=\"/vexplain\"")
}

func TestServerAuthenticatesRequests(t *testing.T) {
	api := &authnFakeServer{}
	s, err := NewServer(api, Options{Authenticator: fakeAuthenticator{}})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	s.ServeHTTP(rec, req)

	require.NotNil(t, api.actor)
	assert.Equal(t, "browser", api.actor.Name)
	assert.Equal(t, http.StatusOK, rec.Code)
}

type authnFakeServer struct {
	fakeVTAdminServer
	actor *rbac.Actor
}

func (f *authnFakeServer) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	f.actor, _ = rbac.FromContext(ctx)
	return &vtadminpb.GetClustersResponse{}, nil
}
