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
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type actionFakeServer struct {
	fakeVTAdminServer
	createKeyspaceRequest *vtadminpb.CreateKeyspaceRequest
}

func (f *actionFakeServer) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	return &vtadminpb.GetClustersResponse{Clusters: []*vtadminpb.Cluster{{Id: "local", Name: "Local"}}}, nil
}

func (f *actionFakeServer) CreateKeyspace(ctx context.Context, req *vtadminpb.CreateKeyspaceRequest) (*vtadminpb.CreateKeyspaceResponse, error) {
	f.createKeyspaceRequest = req
	return &vtadminpb.CreateKeyspaceResponse{Keyspace: &vtadminpb.Keyspace{
		Cluster:  &vtadminpb.Cluster{Id: req.GetClusterId(), Name: "Local"},
		Keyspace: &vtctldatapb.Keyspace{Name: req.GetOptions().GetName()},
	}}, nil
}

func TestCreateKeyspaceFormRendersClusters(t *testing.T) {
	s, err := NewServer(&actionFakeServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/keyspaces/create", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Create keyspace")
	assert.Contains(t, rec.Body.String(), "local")
	assert.Contains(t, rec.Body.String(), "name=\"name\"")
	assert.Contains(t, rec.Body.String(), "name=\"csrf_token\"")
}

func TestCreateKeyspacePostCallsServerAndRedirects(t *testing.T) {
	fake := &actionFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	csrfToken := createKeyspaceCSRFToken(t, s)
	form := url.Values{}
	form.Set("cluster_id", "local")
	form.Set("name", "commerce")
	form.Set("force", "on")
	form.Set("csrf_token", csrfToken)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/keyspaces/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: csrfToken})
	s.ServeHTTP(rec, req)

	require.NotNil(t, fake.createKeyspaceRequest)
	assert.Equal(t, "local", fake.createKeyspaceRequest.GetClusterId())
	assert.Equal(t, "commerce", fake.createKeyspaceRequest.GetOptions().GetName())
	assert.True(t, fake.createKeyspaceRequest.GetOptions().GetForce())
	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Contains(t, rec.Header().Get("Location"), "/keyspace/local/commerce")
	assert.NotContains(t, rec.Header().Get("Location"), "flash=success")
	assert.NotEmpty(t, findCookie(rec, flashCookieName).Value)
}

func TestCreateKeyspacePostMarksFlashCookieSecureOnTLS(t *testing.T) {
	fake := &actionFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	csrfToken := createKeyspaceCSRFToken(t, s)
	form := url.Values{}
	form.Set("cluster_id", "local")
	form.Set("name", "commerce")
	form.Set("csrf_token", csrfToken)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/keyspaces/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: csrfToken})
	req.TLS = &tls.ConnectionState{}
	s.ServeHTTP(rec, req)

	flashCookie := findCookie(rec, flashCookieName)
	require.NotNil(t, flashCookie)
	assert.True(t, flashCookie.Secure)
}

func TestCreateKeyspacePostRejectsMissingCSRFToken(t *testing.T) {
	fake := &actionFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	form := url.Values{}
	form.Set("cluster_id", "local")
	form.Set("name", "commerce")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/keyspaces/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	s.ServeHTTP(rec, req)

	assert.Nil(t, fake.createKeyspaceRequest)
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid CSRF token")
}

func TestCreateKeyspacePostValidatesRequiredFields(t *testing.T) {
	fake := &actionFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	csrfToken := createKeyspaceCSRFToken(t, s)
	form := url.Values{}
	form.Set("cluster_id", "local")
	form.Set("csrf_token", csrfToken)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/keyspaces/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: csrfToken})
	s.ServeHTTP(rec, req)

	assert.Nil(t, fake.createKeyspaceRequest)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Body.String(), "keyspace name is required")
}

func TestCreateKeyspaceHiddenInReadOnlyMode(t *testing.T) {
	s, err := NewServer(&actionFakeServer{}, Options{ReadOnly: true})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/keyspaces/create", nil)
	req.AddCookie(&http.Cookie{Name: flashCookieName, Value: encodeFlash(Flash{Kind: "success", Message: "created keyspace stale"})})
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Body.String(), "read-only")
	assert.NotContains(t, rec.Body.String(), "created keyspace stale")
}

func TestCreateKeyspaceFormSetsCSRFCookieOnlyOnForm(t *testing.T) {
	s, err := NewServer(&actionFakeServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/keyspaces/create", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotNil(t, findCookie(rec, csrfCookieName))
}

func TestCreateKeyspaceFormMarksCSRFCookieSecureOnTLS(t *testing.T) {
	s, err := NewServer(&actionFakeServer{}, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/keyspaces/create", nil)
	req.TLS = &tls.ConnectionState{}
	s.ServeHTTP(rec, req)

	csrfCookie := findCookie(rec, csrfCookieName)
	require.NotNil(t, csrfCookie)
	assert.True(t, csrfCookie.Secure)
}

func createKeyspaceCSRFToken(t *testing.T, s *Server) string {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/keyspaces/create", nil)
	s.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	csrfCookie := findCookie(rec, csrfCookieName)
	require.NotNil(t, csrfCookie)

	matches := regexp.MustCompile(`name="csrf_token" value="([^"]+)"`).FindStringSubmatch(rec.Body.String())
	require.Len(t, matches, 2)
	assert.Equal(t, csrfCookie.Value, matches[1])
	return matches[1]
}

func findCookie(rec *httptest.ResponseRecorder, name string) *http.Cookie {
	for _, cookie := range rec.Result().Cookies() {
		if cookie.Name == name {
			return cookie
		}
	}
	return nil
}
