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

package vtgate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
)

func TestSetSystemVariable(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	exec(t, conn, "set @@sql_mode = 'NO_ZERO_DATE'")
	q := `select str_to_date('00/00/0000', '%m/%d/%Y')`
	assertMatches(t, conn, q, `[[NULL]]`)

	assertMatches(t, conn, "select @@sql_mode", `[[VARCHAR("NO_ZERO_DATE")]]`)
	exec(t, conn, "set @@sql_mode = ''")

	assertMatches(t, conn, q, `[[DATE("0000-00-00")]]`)
}
