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
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

func TestLogErrorsWhenClosing(t *testing.T) {
	original := log.Error
	t.Cleanup(func() { log.Error = original })

	var logMessage string
	log.Error = func(msg string, _ ...slog.Attr) {
		logMessage = msg
	}
	logFunc := LogErrorsWhenClosing(&fakeCloser{})
	logFunc()
	require.Contains(t, logMessage, "test error")
}

type fakeCloser struct{}

func (fc *fakeCloser) Close() error {
	return errors.New("test error")
}
