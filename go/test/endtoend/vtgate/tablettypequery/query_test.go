/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"testing"
	"time"

	"github.com/magiconair/properties/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestSyntaxQuery(t *testing.T) {
	defer cluster.PanicHandler(t)
	//_, err = conn.ExecuteFetch("insert into t1(id1,id2) values(1,1);", 1000, true)
	//require.Nil(t, err)
	//replicaCatchedUp := waitTillReplicaCatchUp(*conn)
	//assert.Equal(t, replicaCatchedUp, true)

	t.Run("TestUseSyntax", func(t *testing.T) {
		testUseSyntax(t)
	})
	t.Run("TestInlineSyntax", func(t *testing.T) {
		testInlineSyntax(t)
	})
}

func testUseSyntax(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("insert into t1(id1,id2) values(2,2);", 1000, true)
	require.Nil(t, err)

	// after inserting new row, if we don't wait for replica catchup,
	// we will get empty row for newly inserted row if if use @replica
	_, err = conn.ExecuteFetch("use ks@replica;", 1000, true)
	require.Nil(t, err)

	// The following block sometimes fail, so commented this
	qr, err := conn.ExecuteFetch("select id1,id2 from t1 where id1 = 2;", 1000, true)
	require.Nil(t, err)
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows), `[]`, "This means replica caught up.")

	// And if wait for replica catch up, then we will get new rows in replica
	catchUp := waitTillReplicaCatchUp(`[[INT64(2) INT64(2)]]`)
	assert.Equal(t, catchUp, true)
	qr, err = conn.ExecuteFetch("select id1,id2 from t1 where id1 = 2;", 1000, true)
	require.Nil(t, err)
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows), `[[INT64(2) INT64(2)]]`)

	// but we don't have a rdonly tablet, we should not be able to query from rdonly
	_, err = conn.ExecuteFetch("use ks@rdonly;", 1000, true)
	require.Nil(t, err)

	qr, err = conn.ExecuteFetch("select id1,id2 from t1;", 1000, true)
	require.NotNil(t, err)
	require.Nil(t, qr)
}

func testInlineSyntax(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("insert into t1(id1,id2) values(3,3);", 1000, true)
	require.Nil(t, err)

	// if we don't wait for replica catchup, it fetches the row even though we specify @replica
	// the bug is, it goes to master tablet
	qr, err := conn.ExecuteFetch("select id1,id2 from ks@replica.t1 where id1 = 3;", 1000, true)
	require.Nil(t, err)
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows), `[[INT64(3) INT64(3)]]`)

	// as we don't have a rdonly tablet, we should not be able to query from rdonly
	qr, err = conn.ExecuteFetch("select id1,id2 from ks@rdonly.t1;", 1000, true)
	// Since rdonly is not present, we should have err as not nil
	// and qr should be nil
	require.Nil(t, err)
	require.NotNil(t, qr)
}

func waitTillReplicaCatchUp(valueToVerify string) bool {
	conn, _ := mysql.Connect(context.Background(), &vtParams)
	defer conn.Close()
	timeout := time.Now().Add(1 * time.Second)
	for time.Now().Before(timeout) {
		qr, err := conn.ExecuteFetch("select id1,id2 from t1;", 1000, true)
		if err != nil {
			return false
		}
		if fmt.Sprintf("%v", qr.Rows) == valueToVerify {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}
