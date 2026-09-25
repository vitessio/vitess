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
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var csrfInputRe = regexp.MustCompile(`name="csrf_token" value="([^"]+)"`)

// renderWithCSRF performs a GET of a page and returns the response along with
// the CSRF token rendered into it. It fails the test if the CSRF cookie or
// the rendered token is missing, or if the rendered token does not match the
// issued cookie — the exact pairing a browser would rely on.
func renderWithCSRF(t *testing.T, s *Server, path string) (token string, rec *httptest.ResponseRecorder) {
	t.Helper()

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	s.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "GET %s did not render", path)

	cookie := findCookie(rec, csrfCookieName)
	require.NotNil(t, cookie, "no CSRF cookie issued for %s", path)
	require.NotEmpty(t, cookie.Value, "CSRF cookie is empty for %s", path)

	match := csrfInputRe.FindStringSubmatch(rec.Body.String())
	require.NotNil(t, match, "no csrf_token input rendered for %s", path)
	require.NotEmpty(t, match[1], "rendered csrf_token value is empty for %s", path)
	require.Equal(t, cookie.Value, match[1], "rendered csrf_token does not match the issued cookie for %s", path)

	return match[1], rec
}

// postFormWithCSRF submits a form with the given CSRF pairing, without
// rewriting the form's token (unlike postShardForm, which uses the fixed
// test pairing). The csrf_token must already be present in the form.
func postFormWithCSRF(s *Server, path, csrfCookie string, form url.Values) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: csrfCookie})
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}
