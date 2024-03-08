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

package opentsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/stats"
)

type mockWriter struct {
	data []*DataPoint
}

func (mw *mockWriter) Write(data []*DataPoint) error {
	mw.data = data
	return nil
}

func TestPushAll(t *testing.T) {
	mw := &mockWriter{}
	b := &backend{
		prefix:     "testPrefix",
		commonTags: map[string]string{"tag1": "value1"},
		writer:     mw,
	}

	err := b.PushAll()
	assert.NoError(t, err)
	before := len(mw.data)

	stats.NewGaugeFloat64("test_push_all1", "help")
	stats.NewGaugeFloat64("test_push_all2", "help")

	err = b.PushAll()
	assert.NoError(t, err)
	after := len(mw.data)

	assert.Equalf(t, after-before, 2, "length of writer.data should have been increased by 2")
}

func TestPushOne(t *testing.T) {
	mw := &mockWriter{}
	b := &backend{
		prefix:     "testPrefix",
		commonTags: map[string]string{"tag1": "value1"},
		writer:     mw,
	}

	s := stats.NewGaugeFloat64("test_push_one", "help")
	err := b.PushOne("test_push_one", s)
	assert.NoError(t, err)

	assert.Len(t, mw.data, 1)
	assert.Equal(t, "testprefix.test_push_one", mw.data[0].Metric)
}
