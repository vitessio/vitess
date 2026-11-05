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

package legacy

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log/logtest"
)

func TestReopenLogsCapacity(t *testing.T) {
	rp := NewResourcePool(nil, 2, 2, 0, 0, nil, nil, 0)
	t.Cleanup(rp.Close)

	logs := logtest.Capture(t, slog.LevelInfo)
	rp.reopen()

	record := logs.Record(t, "Draining and reopening resource pool by request")
	assert.Equal(t, "INFO", record["level"])
	assert.EqualValues(t, 2, record["capacity"])
}
