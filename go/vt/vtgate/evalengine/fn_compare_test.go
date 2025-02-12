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
