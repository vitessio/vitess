/*
Copyright 2023 The Vitess Authors.

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

package debug

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/viper"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/viperutil/internal/registry"
)

// HandlerFunc provides an http.HandlerFunc that renders the combined config
// registry (both static and dynamic) for debugging purposes.
//
// By default, this writes the config in viper's "debug" format (what you get
// if you call viper.Debug()). If the query parameter "format" is present, and
// matches one of viper's supported config extensions (case-insensitively), the
// combined config will be written to the response in that format.
//
// Example requests:
//   - GET /debug/config
//   - GET /debug/config?format=json
//   - GET /debug/config?format=yaml
func HandlerFunc(w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}

	v := registry.Combined()
	format := strings.ToLower(r.URL.Query().Get("format"))
	switch {
	case format == "":
		v.DebugTo(w)
	case slices2.Any(viper.SupportedExts, func(ext string) bool { return ext == format }):
		// Got a supported format; write the config to a tempfile in that format,
		// then copy it to the response.
		//
		// (Sadly, viper does not yet have a WriteConfigTo(w io.Writer), so we have
		// to do this little hacky workaround).
		v.SetConfigType(format)
		tmp, err := os.CreateTemp("", "viper_debug")
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to render config to tempfile: %v", err), http.StatusInternalServerError)
			return
		}
		defer os.Remove(tmp.Name())

		if err := v.WriteConfigAs(tmp.Name()); err != nil {
			http.Error(w, fmt.Sprintf("failed to render config to tempfile: %v", err), http.StatusInternalServerError)
			return
		}

		if _, err := io.Copy(w, tmp); err != nil {
			http.Error(w, fmt.Sprintf("failed to write rendered config: %v", err), http.StatusInternalServerError)
			return
		}
	default:
		http.Error(w, fmt.Sprintf("unsupported config format %s", format), http.StatusBadRequest)
	}
}
