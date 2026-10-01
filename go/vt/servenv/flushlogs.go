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

package servenv

import (
	"fmt"
	"net/http"

	"vitess.io/vitess/go/vt/log"
)

// init registers the deprecated /debug/flushlogs endpoint. v24 did not show a deprecation warning for it, so it
// stays as a no-op until v26.
func init() {
	HTTPHandleFunc("/debug/flushlogs", flushLogs)
}

// flushLogs responds with success and logs a deprecation warning. The logger writes each record immediately.
func flushLogs(w http.ResponseWriter, _ *http.Request) {
	log.Warn("/debug/flushlogs is deprecated, it has no effect and will be removed in v26")
	fmt.Fprint(w, "flushed")
}
