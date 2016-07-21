// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"net/http"
)

// This is a separate file so it can be selectively included/excluded from
// builds to opt in/out of the redirect.

func init() {
	// Anything unrecognized gets redirected to the status page.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/debug/status", http.StatusFound)
	})
}
