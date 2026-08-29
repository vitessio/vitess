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

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	themeCookieName          = "vtadmin2_theme"
	defaultClusterCookieName = "vtadmin2_default_cluster"
)

// validThemes are the supported theme values for the theme cookie.
var validThemes = map[string]bool{
	"light":  true,
	"dark":   true,
	"system": true,
}

type (
	settingsData struct {
		Clusters       []*vtadminpb.Cluster
		Theme          string
		DefaultCluster string
	}
)

// settingsForm renders the user preferences page: theme and default cluster,
// persisted in cookies.
func (s *Server) settingsForm(w http.ResponseWriter, r *http.Request) {
	clustersResp, err := s.api.GetClusters(r.Context(), &vtadminpb.GetClustersRequest{})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Settings", err)
		return
	}

	theme := "system"
	if cookie, err := r.Cookie(themeCookieName); err == nil && validThemes[cookie.Value] {
		theme = cookie.Value
	}

	s.render(w, r, http.StatusOK, "settings.html", PageData{
		Title:     "Settings",
		Active:    "settings",
		NeedsCSRF: true,
		Data: settingsData{
			Clusters:       clustersResp.GetClusters(),
			Theme:          theme,
			DefaultCluster: cookieValue(r, defaultClusterCookieName),
		},
	})
}

func (s *Server) settingsSave(w http.ResponseWriter, r *http.Request) {
	const title = "Settings"
	// Preferences are local browser cookies, not Vitess mutations. They remain
	// changeable in read-only mode, while still requiring CSRF protection.
	if err := r.ParseForm(); err != nil {
		s.renderFormError(w, r, title, err.Error())
		return
	}
	if !validCSRFToken(r) {
		s.renderError(w, r, http.StatusForbidden, title, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "invalid CSRF token"))
		return
	}

	theme := r.Form.Get("theme")
	if !validThemes[theme] {
		s.renderFormError(w, r, title, "invalid theme: "+theme)
		return
	}

	secure := r.TLS != nil
	http.SetCookie(w, &http.Cookie{
		Name:     themeCookieName,
		Value:    theme,
		Path:     "/",
		Secure:   secure,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	if cluster := r.Form.Get("default_cluster"); cluster != "" {
		http.SetCookie(w, &http.Cookie{
			Name:     defaultClusterCookieName,
			Value:    cluster,
			Path:     "/",
			Secure:   secure,
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		})
	} else {
		http.SetCookie(w, &http.Cookie{
			Name:     defaultClusterCookieName,
			Value:    "",
			Path:     "/",
			MaxAge:   -1,
			Secure:   secure,
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		})
	}

	redirectWithFlash(w, r, "/settings", Flash{
		Kind:    "success",
		Message: "settings saved",
	})
}

func cookieValue(r *http.Request, name string) string {
	if cookie, err := r.Cookie(name); err == nil {
		return cookie.Value
	}
	return ""
}
