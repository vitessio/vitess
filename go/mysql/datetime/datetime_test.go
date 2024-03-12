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

package datetime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var testGoTime = time.Date(2024, 03, 12, 12, 30, 20, 59, time.UTC)

func TestNewTimeFromStd(t *testing.T) {
	time := NewTimeFromStd(testGoTime)

	assert.Equal(t, uint16(12), time.hour)
	assert.Equal(t, uint8(30), time.minute)
	assert.Equal(t, uint8(20), time.second)
	assert.Equal(t, uint32(59), time.nanosecond)
}

func TestNewDateFromStd(t *testing.T) {
	date := NewDateFromStd(testGoTime)

	assert.Equal(t, uint16(2024), date.year)
	assert.Equal(t, uint8(03), date.month)
	assert.Equal(t, uint8(12), date.day)
}

func TestNewDateTimeFromStd(t *testing.T) {
	dt := NewDateTimeFromStd(testGoTime)

	assert.Equal(t, uint16(2024), dt.Date.year)
	assert.Equal(t, uint8(03), dt.Date.month)
	assert.Equal(t, uint8(12), dt.Date.day)

	assert.Equal(t, uint16(12), dt.Time.hour)
	assert.Equal(t, uint8(30), dt.Time.minute)
	assert.Equal(t, uint8(20), dt.Time.second)
	assert.Equal(t, uint32(59), dt.Time.nanosecond)
}

func TestAppendFormat(t *testing.T) {
	time := NewTimeFromStd(testGoTime)

	b := []byte("test:")
	nb := time.AppendFormat(b, 0)
	assert.Equal(t, []byte("test:12:30:20"), nb)

	nb = time.AppendFormat(b, 1)
	assert.Equal(t, []byte("test:12:30:20.0"), nb)

	nb = time.AppendFormat(b, 3)
	assert.Equal(t, []byte("test:12:30:20.000"), nb)

	// TODO: Add tests for non-zero decimal part
	// TODO: Add tests for neg hour
}
