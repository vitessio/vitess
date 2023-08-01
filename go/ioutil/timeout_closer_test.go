/*
Copyright 2023 The Vitess Authors.

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
	"github.com/stretchr/testify/require"
)

type hangCloser struct {
}

func (c hangCloser) Close() error {
	ch := make(chan bool)
	ch <- true // hang forever
	return nil
}

func TestTimeoutCloser(t *testing.T) {
	closer := NewTimeoutCloser(&hangCloser{}, time.Second)
	err := closer.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
}
