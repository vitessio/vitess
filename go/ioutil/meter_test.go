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

package ioutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	calledBytes    int
	calledDuration time.Duration
)

func testfn(b int, d time.Duration) {
	calledBytes = b
	calledDuration = d
}

func TestMeter(t *testing.T) {
	tm := meter{
		fs:       []func(b int, d time.Duration){testfn},
		bytes:    123,
		duration: time.Second,
	}

	assert.Equal(t, int64(123), tm.Bytes())
	assert.Equal(t, time.Second, tm.Duration())

	tf := func(p []byte) (int, error) {
		return 1, nil
	}

	b, err := tm.measure(tf, []byte(""))
	wantDuration := time.Second + calledDuration
	wantBytes := int64(123) + int64(calledBytes)

	assert.NoError(t, err)
	assert.Equal(t, 1, b)
	assert.Equal(t, wantDuration, tm.duration)
	assert.Equal(t, wantBytes, tm.bytes)
}
