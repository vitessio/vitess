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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockWriter struct {
	data []*DataPoint
}

func (mw *mockWriter) Write(data []*DataPoint) error {
	mw.data = data
	return nil
}

type mockVariable struct{}

func (mv *mockVariable) Help() string {
	return ""
}

func (mv *mockVariable) String() string {
	return ""
}

func TestPushAll(t *testing.T) {
	mw := &mockWriter{}
	p := "testPrefix"
	ct := map[string]string{
		"tag1": "value1",
		"tag2": "value2",
	}

	b := &backend{
		prefix:     p,
		commonTags: ct,
		writer:     mw,
	}

	// TODO: Add asserts
	err := b.PushAll()
	assert.NoError(t, err)
}

func TestPushOne(t *testing.T) {
	mw := &mockWriter{}
	mv := &mockVariable{}
	b := &backend{
		prefix:     "test",
		commonTags: map[string]string{"tag1": "value1"},
		writer:     mw,
	}

	// TODO: Add asserts
	err := b.PushOne("var1", mv)
	assert.NoError(t, err)

	fmt.Println(b.collector().data)
}
