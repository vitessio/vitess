/*
Copyright 2020 The Vitess Authors.

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

package endtoend

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

// TestReserveFlushTables checks that `flush table with read lock` works only with reserve api.
func TestReserveFlushTables(t *testing.T) {
	client := framework.NewClient()

	_, err := client.Execute("flush tables with read lock", nil)
	assert.ErrorContains(t, err, "Flush not allowed without reserved connection")

	_, err = client.Execute("unlock tables", nil)
	assert.ErrorContains(t, err, "unlock tables should be executed with an existing connection")

	_, err = client.ReserveExecute("flush tables with read lock", nil, nil)
	assert.NoError(t, err)

	_, err = client.Execute("unlock tables", nil)
	assert.NoError(t, err)

	assert.NoError(t,
		client.Release())
}
