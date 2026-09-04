/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 the "License";
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
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func TestSettingsFormRenders(t *testing.T) {
	fake := &settingsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/settings", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, `name="theme"`)
	assert.Contains(t, body, `value="light"`)
	assert.Contains(t, body, `value="dark"`)
	assert.Contains(t, body, `value="system"`)
	assert.Contains(t, body, `name="default_cluster"`)
	assert.Contains(t, body, `value="local"`)
	assert.Contains(t, body, `name="csrf_token"`)
}

func TestSettingsPostSavesCookies(t *testing.T) {
	fake := &settingsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	form := url.Values{
		"theme":           {"dark"},
		"default_cluster": {"local"},
	}
	rec := postShardForm(t, s, "/settings", form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)

	themeCookie := findCookie(rec, themeCookieName)
	require.NotNil(t, themeCookie)
	assert.Equal(t, "dark", themeCookie.Value)

	clusterCookie := findCookie(rec, defaultClusterCookieName)
	require.NotNil(t, clusterCookie)
	assert.Equal(t, "local", clusterCookie.Value)
}

func TestSettingsPostAllowedInReadOnlyMode(t *testing.T) {
	fake := &settingsFakeServer{}
	s, err := NewServer(fake, Options{ReadOnly: true})
	require.NoError(t, err)

	rec := postShardForm(t, s, "/settings", url.Values{"theme": {"dark"}})

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	themeCookie := findCookie(rec, themeCookieName)
	require.NotNil(t, themeCookie)
	assert.Equal(t, "dark", themeCookie.Value)
}

func TestSettingsPostRejectsInvalidTheme(t *testing.T) {
	fake := &settingsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	form := url.Values{"theme": {"hot-dog-stand"}}
	rec := postShardForm(t, s, "/settings", form)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Nil(t, findCookie(rec, themeCookieName))
}

func TestPageRenderAppliesThemeCookie(t *testing.T) {
	fake := &settingsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	req.AddCookie(&http.Cookie{Name: themeCookieName, Value: "dark"})
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `data-theme="dark"`)
}

func TestPageRenderDefaultsToSystemTheme(t *testing.T) {
	fake := &settingsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `data-theme="system"`)
}

func TestNavLinksToSettings(t *testing.T) {
	fake := &settingsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/clusters", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `href="/settings"`)
}

type settingsFakeServer struct {
	fakeVTAdminServer
}

func (f *settingsFakeServer) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	return &vtadminpb.GetClustersResponse{Clusters: []*vtadminpb.Cluster{
		{Id: "local", Name: "Local"},
		{Id: "prod", Name: "Production"},
	}}, nil
}
