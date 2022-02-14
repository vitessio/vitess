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
