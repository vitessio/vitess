/*
Copyright 2022 The Vitess Authors.

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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

func TestNoConnectionReservationOnSettings(t *testing.T) {
	framework.Server.Config().EnableSettingsPool = true
	defer func() {
		framework.Server.Config().EnableSettingsPool = false
	}()

	client := framework.NewClient()
	defer client.Release()

	query := "select @@sql_mode"
	setting := "set @@sql_mode = ''"

	qr, err := client.ReserveExecute(query, []string{setting}, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 0, client.ReservedID())
	assert.Equal(t, `[[VARCHAR("")]]`, fmt.Sprintf("%v", qr.Rows))

	qr, err = client.ReserveStreamExecute(query, []string{setting}, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 0, client.ReservedID())
	assert.Equal(t, `[[VARCHAR("")]]`, fmt.Sprintf("%v", qr.Rows))
}
