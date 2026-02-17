/*
Copyright 2024 The Vitess Authors.

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

package trace

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log"
)

// If captureStdout is false, it will capture the outut of
// os.Stderr
func captureOutput(t *testing.T, f func(), captureStdout bool) string {
	oldVal := os.Stderr
	if captureStdout {
		oldVal = os.Stdout
	}
	t.Cleanup(func() {
		// Ensure reset even if deferred function panics
		if captureStdout {
			os.Stdout = oldVal
		} else {
			os.Stderr = oldVal
		}
	})

	r, w, _ := os.Pipe()
	if captureStdout {
		os.Stdout = w
	} else {
		os.Stderr = w
	}

	f()

	w.Close()
	got, _ := io.ReadAll(r)

	return string(got)
}

func TestLoggerLogAndError(t *testing.T) {
	logger := traceLogger{}
	original := log.Error
	t.Cleanup(func() { log.Error = original })

	var logMessage string
	log.Error = func(msg string, _ ...slog.Attr) {
		logMessage = msg
	}
	// Test Error() output
	logger.Error("This is an error message")
	assert.Contains(t, logMessage, "This is an error message")
	// Test Log() output
	logMessage = ""
	logger.Log("This is an log message")
	assert.Contains(t, logMessage, "This is an log message")
}
