/*
Copyright 2025 The Vitess Authors.

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

package evalengine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/datetime"
)

func TestCompareTemporal(t *testing.T) {
	tests := []struct {
		name   string
		val1   *evalTemporal
		val2   *evalTemporal
		result int
	}{
		{
			name:   "equal values",
			val1:   newEvalDateTime(datetime.NewDateTimeFromStd(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), 6, false),
			val2:   newEvalDateTime(datetime.NewDateTimeFromStd(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), 6, false),
			result: 0,
		},
		{
			name:   "larger value",
			val1:   newEvalDateTime(datetime.NewDateTimeFromStd(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), 6, false),
			val2:   newEvalDateTime(datetime.NewDateTimeFromStd(time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)), 6, false),
			result: -1,
		},
		{
			name:   "smaller value",
			val1:   newEvalDateTime(datetime.NewDateTimeFromStd(time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)), 6, false),
			val2:   newEvalDateTime(datetime.NewDateTimeFromStd(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), 6, false),
			result: 1,
		},
		{
			name:   "first nil value",
			val1:   nil,
			val2:   newEvalDateTime(datetime.NewDateTimeFromStd(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), 6, false),
			result: 1,
		},

		{
			name:   "second nil value",
			val1:   newEvalDateTime(datetime.NewDateTimeFromStd(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), 6, false),
			val2:   nil,
			result: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx1 := 0
			idx2 := 1
			if tt.val1 == nil {
				idx1 = -1
			}
			if tt.val2 == nil {
				idx2 = -1
			}
			assert.Equal(t, tt.result, compareTemporal([]*evalTemporal{tt.val1, tt.val2}, idx1, idx2))
		})
	}
}
