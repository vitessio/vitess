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

type mockWriteCloser struct{}

func (*mockWriteCloser) Write([]byte) (int, error) {
	return 2, nil
}

func (*mockWriteCloser) Close() error {
	return nil
}

type mockWrite struct{}

func (*mockWrite) Write([]byte) (int, error) {
	return 3, nil
}

func TestMeteredWriter(t *testing.T) {
	mwc := NewMeteredWriteCloser(&mockWriteCloser{}, testfn)
	n, err := mwc.Write([]byte(""))
	assert.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, 2, calledBytes)
	assert.Equal(t, calledDuration, mwc.Duration())

	mw := NewMeteredWriter(&mockWrite{}, testfn)
	n, err = mw.Write([]byte(""))
	assert.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, 3, calledBytes)
	assert.Equal(t, calledDuration, mw.Duration())
}
