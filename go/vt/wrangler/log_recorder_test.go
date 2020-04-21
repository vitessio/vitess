/*
Copyright 2020 The Vitess Authors.

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

package wrangler

import (
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestLogRecorder(t *testing.T) {
	lr := NewLogRecorder()
	lr.Log("log 1")
	lr.Log("log 2")
	lr.LogSlice([]string{"log 4", "log 3"})
	want := []string{"log 1", "log 2", "log 3", "log 4"}
	assert.Equal(t, lr.GetLogs(), want)
}
