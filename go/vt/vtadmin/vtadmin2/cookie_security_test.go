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
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getSettingsWithCSRF renders the settings page (which mints a CSRF cookie)
// and returns the CSRF cookie for cookie-security assertions.
func getSettingsWithCSRF(t *testing.T, s *Server, header http.Header) *http.Cookie {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/settings", nil)
	if header != nil {
		req.Header = header
	}
	s.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	cookie := findCookie(rec, csrfCookieName)
	require.NotNil(t, cookie, "no CSRF cookie issued")
	return cookie
}

func TestCSRFCookieNotSecureOverPlainHTTP(t *testing.T) {
	fake := &settingsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	cookie := getSettingsWithCSRF(t, s, nil)

	assert.False(t, cookie.Secure, "cookies must not be Secure over plain HTTP")
}

func TestCSRFCookieSecureOverDirectTLS(t *testing.T) {
	fake := &settingsFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/settings", nil)
	req.TLS = &tls.ConnectionState{} // non-nil signals direct TLS
	s.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	cookie := findCookie(rec, csrfCookieName)
	require.NotNil(t, cookie)
	assert.True(t, cookie.Secure, "cookies must be Secure over direct TLS")
}

func TestCSRFCookieBehindTrustedProxy(t *testing.T) {
	fake := &settingsFakeServer{}

	t.Run("enabled trusts X-Forwarded-Proto https", func(t *testing.T) {
		s, err := NewServer(fake, Options{TrustProxyProto: true})
		require.NoError(t, err)

		cookie := getSettingsWithCSRF(t, s, http.Header{
			"X-Forwarded-Proto": []string{"https"},
		})

		assert.True(t, cookie.Secure, "cookies must be Secure behind a trusted HTTPS proxy")
	})

	t.Run("enabled ignores plaintext forwarded proto", func(t *testing.T) {
		s, err := NewServer(fake, Options{TrustProxyProto: true})
		require.NoError(t, err)

		cookie := getSettingsWithCSRF(t, s, http.Header{
			"X-Forwarded-Proto": []string{"http"},
		})

		assert.False(t, cookie.Secure, "X-Forwarded-Proto: http must not make cookies Secure")
	})

	t.Run("disabled ignores forwarded header", func(t *testing.T) {
		s, err := NewServer(fake, Options{})
		require.NoError(t, err)

		cookie := getSettingsWithCSRF(t, s, http.Header{
			"X-Forwarded-Proto": []string{"https"},
		})

		assert.False(t, cookie.Secure, "X-Forwarded-Proto must be ignored unless TrustProxyProto is enabled")
	})
}
