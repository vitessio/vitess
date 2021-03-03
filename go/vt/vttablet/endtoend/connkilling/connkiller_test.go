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

/*
All tests in this package come with a three second time out for OLTP session
*/
package connkilling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

func TestTxKillerKillsTransactionsInReservedConnections(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	_, err := client.ReserveBeginExecute("select 42", nil, nil)
	require.NoError(t, err)

	assertIsKilledWithin5Seconds(t, client)
}

func TestTxKillerDoesNotKillReservedConnectionsInUse(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	_, err := client.ReserveExecute("select 42", nil, nil)
	require.NoError(t, err)

	assertIsNotKilledOver5Second(t, client)
}

func TestTxKillerCountsTimeFromTxStartedNotStatefulConnCreated(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	// reserve connection at 0th second
	_, err := client.ReserveExecute("select 42", nil, nil)
	require.NoError(t, err)

	// elapsed 2 seconds
	time.Sleep(2 * time.Second)

	// update the timer on tx start - new tx timer starts
	_, err = client.BeginExecute("select 44", nil, nil)
	require.NoError(t, err)

	// elapsed 1 second from tx and 3 second from reserved conn.
	time.Sleep(1 * time.Second)
	_, err = client.Execute("select 43", nil)
	require.NoError(t, err)

	// elapsed 2 second from tx and 4 second from reserved conn. It does not fail.
	time.Sleep(1 * time.Second)
	_, err = client.Execute("select 43", nil)
	require.NoError(t, err)

	assertIsKilledWithin5Seconds(t, client)
}

func TestTxKillerKillsTransactionThreeSecondsAfterCreation(t *testing.T) {
	client := framework.NewClient()
	defer client.Release()

	_, err := client.BeginExecute("select 42", nil, nil)
	require.NoError(t, err)

	assertIsKilledWithin5Seconds(t, client)
}

func assertIsNotKilledOver5Second(t *testing.T, client *framework.QueryClient) {
	for i := 0; i < 5; i++ {
		_, err := client.Execute("select 43", nil)
		require.NoError(t, err)
		time.Sleep(1 * time.Second)
	}
}

func assertIsKilledWithin5Seconds(t *testing.T, client *framework.QueryClient) {
	var err error
	// when it is used once per second
	for i := 0; i < 5; i++ {
		_, err = client.Execute("select 43", nil)
		if err != nil {
			break
		}
		time.Sleep(1 * time.Second)
	}

	// then it should still be killed. transactions are tracked per tx-creation time and not last-used time
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeded timeout: 3s")
}
