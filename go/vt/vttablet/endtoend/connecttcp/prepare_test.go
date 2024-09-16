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

package connecttcp

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

// TestPrepareOnTCP tests that a prepare statement is not allowed on a network connection.
func TestPrepareOnTCP(t *testing.T) {
	client := framework.NewClient()

	query := "insert into vitess_test (intval) values(4)"

	err := client.Begin(false)
	require.NoError(t, err)

	_, err = client.Execute(query, nil)
	require.NoError(t, err)

	err = client.Prepare("aa")
	require.ErrorContains(t, err, "VT10002: atomic distributed transaction not allowed: cannot prepare the transaction on a network connection")
}
