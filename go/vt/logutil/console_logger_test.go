/*
Copyright 2019 The Vitess Authors.

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

package logutil

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

func TestConsoleLogger(t *testing.T) {
	testConsoleLogger(t, false)
}

func TestTeeConsoleLogger(t *testing.T) {
	testConsoleLogger(t, true)
}

func testConsoleLogger(t *testing.T, tee bool) {
	originalInfoDepth := log.InfoDepth
	originalWarnDepth := log.WarnDepth
	originalErrorDepth := log.ErrorDepth

	t.Cleanup(func() {
		log.InfoDepth = originalInfoDepth
		log.WarnDepth = originalWarnDepth
		log.ErrorDepth = originalErrorDepth
	})

	var messages []string

	log.InfoDepth = func(_ int, msg string, _ ...slog.Attr) {
		messages = append(messages, msg)
	}
	log.WarnDepth = func(_ int, msg string, _ ...slog.Attr) {
		messages = append(messages, msg)
	}
	log.ErrorDepth = func(_ int, msg string, _ ...slog.Attr) {
		messages = append(messages, msg)
	}

	var logger Logger
	if tee {
		logger = NewTeeLogger(NewConsoleLogger(), NewMemoryLogger())
	} else {
		logger = NewConsoleLogger()
	}

	logger.Infof("info %v %v", 1, tee)
	logger.Warningf("warning %v %v", 2, tee)
	logger.Errorf("error %v %v", 3, tee)

	require.Len(t, messages, 3)
	require.Equal(t, fmt.Sprintf("info 1 %v", tee), messages[0])
	require.Equal(t, fmt.Sprintf("warning 2 %v", tee), messages[1])
	require.Equal(t, fmt.Sprintf("error 3 %v", tee), messages[2])
}
