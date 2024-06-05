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

	"github.com/stretchr/testify/assert"
)

type mockReadCloser struct{}

func (*mockReadCloser) Read([]byte) (int, error) {
	return 2, nil
}

func (*mockReadCloser) Close() error {
	return nil
}

type mockRead struct{}

func (*mockRead) Read([]byte) (int, error) {
	return 3, nil
}

func TestMeteredReader(t *testing.T) {
	mrc := NewMeteredReadCloser(&mockReadCloser{}, testfn)
	n, err := mrc.Read([]byte(""))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, 2, calledBytes)
	assert.Equal(t, calledDuration, mrc.Duration())

	mr := NewMeteredReader(&mockRead{}, testfn)
	n, err = mr.Read([]byte(""))
	assert.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, 3, calledBytes)
	assert.Equal(t, calledDuration, mr.Duration())
}
