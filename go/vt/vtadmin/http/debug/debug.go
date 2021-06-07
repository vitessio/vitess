/*
Copyright 2021 The Vitess Authors.

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
	"net/http"
	"os"
	"sort"
	"strings"

	"vitess.io/vitess/go/flagutil"
)

var (
	// SanitizeEnv is the set of environment variables to sanitize their values
	// in the Env http handler.
	SanitizeEnv flagutil.StringSetFlag
	// OmitEnv is the set of environment variables to omit entirely in the Env
	// http handler.
	OmitEnv flagutil.StringSetFlag
)

const sanitized = "********"

// Env responds with a plaintext listing of key=value pairs of the environment
// variables, sorted by key name.
//
// If a variable appears in OmitEnv, it is excluded entirely. If a variable
// appears in SanitizeEnv, its value is replaced with a sanitized string,
// including if there was no value set in the environment.
func Env(w http.ResponseWriter, r *http.Request) {
	vars := readEnv()

	msg := &strings.Builder{}
	for i, kv := range vars {
		msg.WriteString(fmt.Sprintf("%s=%s", kv[0], kv[1]))
		if i < len(vars)-1 {
			msg.WriteByte('\n')
		}
	}

	w.Write([]byte(msg.String()))
}

func readEnv() [][2]string {
	env := os.Environ()
	vars := make([][2]string, 0, len(env))

	var key, value string
	for _, ev := range env {
		parts := strings.SplitN(ev, "=", 2)
		switch len(parts) {
		case 0:
			key = ev
		case 1:
			key = parts[0]
		default:
			key = parts[0]
			value = parts[1]
		}

		if key == "" {
			continue
		}

		if OmitEnv.ToSet().Has(key) {
			continue
		}

		if SanitizeEnv.ToSet().Has(key) {
			value = sanitized
		}

		vars = append(vars, [2]string{
			key,
			value,
		})
	}

	// Sort by env var name, ascending.
	sort.SliceStable(vars, func(i, j int) bool {
		left, right := vars[i], vars[j]
		return left[0] < right[0]
	})

	return vars
}
