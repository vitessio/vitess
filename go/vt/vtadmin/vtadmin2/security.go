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
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"strings"
)

const (
	csrfCookieName  = "vtadmin2_csrf"
	flashCookieName = "vtadmin2_flash"
)

func csrfToken(w http.ResponseWriter, r *http.Request) string {
	if cookie, err := r.Cookie(csrfCookieName); err == nil && cookie.Value != "" {
		return cookie.Value
	}

	token := randomToken()
	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookieName,
		Value:    token,
		Path:     "/",
		Secure:   r.TLS != nil,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
	return token
}

func validCSRFToken(r *http.Request) bool {
	cookie, err := r.Cookie(csrfCookieName)
	if err != nil || cookie.Value == "" {
		return false
	}
	formToken := r.Form.Get("csrf_token")
	if formToken == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(formToken)) == 1
}

func setFlash(w http.ResponseWriter, r *http.Request, flash Flash) {
	http.SetCookie(w, &http.Cookie{
		Name:     flashCookieName,
		Value:    encodeFlash(flash),
		Path:     "/",
		Secure:   r.TLS != nil,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

func flashFromRequest(w http.ResponseWriter, r *http.Request) *Flash {
	cookie, err := r.Cookie(flashCookieName)
	if err != nil || cookie.Value == "" {
		return nil
	}

	clearFlash(w, r)
	flash := decodeFlash(cookie.Value)
	if flash == nil || !validFlashKind(flash.Kind) || flash.Message == "" {
		return nil
	}
	return flash
}

func clearFlash(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     flashCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		Secure:   r.TLS != nil,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

func encodeFlash(flash Flash) string {
	return base64.RawURLEncoding.EncodeToString([]byte(flash.Kind)) + "." + base64.RawURLEncoding.EncodeToString([]byte(flash.Message))
}

func decodeFlash(value string) *Flash {
	kindValue, messageValue, ok := strings.Cut(value, ".")
	if !ok {
		return nil
	}
	kind, err := base64.RawURLEncoding.DecodeString(kindValue)
	if err != nil {
		return nil
	}
	message, err := base64.RawURLEncoding.DecodeString(messageValue)
	if err != nil {
		return nil
	}
	return &Flash{Kind: string(kind), Message: string(message)}
}

func validFlashKind(kind string) bool {
	return kind == "success" || kind == "error"
}

func randomToken() string {
	var buf [32]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(buf[:])
}
