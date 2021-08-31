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

package endtoend

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

func TestStreamUnion(t *testing.T) {
	qr, err := framework.NewClient().StreamExecute("select 1 from dual union select 1 from dual", nil)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, 1, len(qr.Rows))
}

func populateStressQuery(client *framework.QueryClient, rowCount int, rowContent string) error {
	err := client.Begin(false)
	if err != nil {
		return err
	}
	defer client.Rollback()

	for i := 0; i < rowCount; i++ {
		query := fmt.Sprintf("insert into vitess_stress values (%d, '%s')", i, strings.Repeat(rowContent, 2048/len(rowContent)))
		_, err := client.Execute(query, nil)
		if err != nil {
			return err
		}
	}
	return client.Commit()
}

func BenchmarkStreamQuery(b *testing.B) {
	const RowCount = 1100
	const RowContent = "abcdefghijklmnopqrstuvwxyz"

	client := framework.NewClient()
	err := populateStressQuery(client, RowCount, RowContent)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Execute("delete from vitess_stress", nil)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := client.Stream("select * from vitess_stress", nil, func(result *sqltypes.Result) error {
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestStreamConsolidation(t *testing.T) {
	const Workers = 50
	const RowCount = 1100
	const RowContent = "abcdefghijklmnopqrstuvwxyz"

	client := framework.NewClient()
	err := populateStressQuery(client, RowCount, RowContent)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Execute("delete from vitess_stress", nil)

	defaultPoolSize := framework.Server.StreamPoolSize()

	framework.Server.SetStreamPoolSize(4)
	framework.Server.SetStreamConsolidationBlocking(true)

	defer func() {
		framework.Server.SetStreamPoolSize(defaultPoolSize)
		framework.Server.SetStreamConsolidationBlocking(false)
	}()

	var start = make(chan struct{})
	var finish sync.WaitGroup

	// Spawn N workers at the same time to stress test the stream consolidator
	for i := 0; i < Workers; i++ {
		finish.Add(1)
		go func() {
			defer finish.Done()

			// block all the workers so they all perform their queries at the same time
			<-start

			var rowCount int
			err := client.Stream("select * from vitess_stress", nil, func(result *sqltypes.Result) error {
				for _, r := range result.Rows {
					rowCount += len(r)
				}
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 2200, rowCount)
		}()
	}

	// wait until all the goroutines have spawned and are blocked before we unblock them at once
	time.Sleep(500 * time.Millisecond)
	close(start)
	finish.Wait()
}

func TestStreamBigData(t *testing.T) {
	client := framework.NewClient()
	err := populateBigData(client)
	if err != nil {
		t.Error(err)
		return
	}
	defer client.Execute("delete from vitess_big", nil)

	qr, err := client.StreamExecute("select * from vitess_big b1, vitess_big b2 order by b1.id, b2.id", nil)
	if err != nil {
		t.Error(err)
		return
	}
	row10 := framework.RowsToStrings(qr)[10]
	want := []string{
		"0",
		"AAAAAAAAAAAAAAAAAA 0",
		"BBBBBBBBBBBBBBBBBB 0",
		"C",
		"DDDDDDDDDDDDDDDDDD 0",
		"EEEEEEEEEEEEEEEEEE 0",
		"FF 0",
		"GGGGGGGGGGGGGGGGGG 0",
		"0",
		"0",
		"0",
		"0",
		"10",
		"AAAAAAAAAAAAAAAAAA 10",
		"BBBBBBBBBBBBBBBBBB 10",
		"C",
		"DDDDDDDDDDDDDDDDDD 10",
		"EEEEEEEEEEEEEEEEEE 10",
		"FF 10",
		"GGGGGGGGGGGGGGGGGG 10",
		"10",
		"10",
		"10",
		"10"}
	if !reflect.DeepEqual(row10, want) {
		t.Errorf("Row10: \n%#v, want \n%#v", row10, want)
	}
}

func TestStreamTerminate(t *testing.T) {
	client := framework.NewClient()
	err := populateBigData(client)
	if err != nil {
		t.Error(err)
		return
	}
	defer client.Execute("delete from vitess_big", nil)

	called := false
	err = client.Stream(
		"select * from vitess_big b1, vitess_big b2 order by b1.id, b2.id",
		nil,
		func(*sqltypes.Result) error {
			if !called {
				queries := framework.LiveQueryz()
				if l := len(queries); l != 1 {
					t.Errorf("len(queries): %d, want 1", l)
					return errors.New("no queries from LiveQueryz")
				}
				err := framework.StreamTerminate(queries[0].ConnID)
				if err != nil {
					return err
				}
				called = true
			}
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	)
	if code := vterrors.Code(err); code != vtrpcpb.Code_CANCELED {
		t.Errorf("Errorcode: %v, want %v", code, vtrpcpb.Code_CANCELED)
	}
}

func populateBigData(client *framework.QueryClient) error {
	err := client.Begin(false)
	if err != nil {
		return err
	}
	defer client.Rollback()

	for i := 0; i < 100; i++ {
		stri := strconv.Itoa(i)
		query := "insert into vitess_big values " +
			"(" + stri + ", " +
			"'AAAAAAAAAAAAAAAAAA " + stri + "', " +
			"'BBBBBBBBBBBBBBBBBB " + stri + "', " +
			"'C', " +
			"'DDDDDDDDDDDDDDDDDD " + stri + "', " +
			"'EEEEEEEEEEEEEEEEEE " + stri + "', " +
			"'FF " + stri + "', " +
			"'GGGGGGGGGGGGGGGGGG " + stri + "', " +
			stri + ", " +
			stri + ", " +
			stri + ", " +
			stri + ")"
		_, err := client.Execute(query, nil)
		if err != nil {
			return err
		}
	}
	return client.Commit()
}

func TestStreamError(t *testing.T) {
	_, err := framework.NewClient().StreamExecute("select count(abcd) from vitess_big", nil)
	want := "Unknown column"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, must start with %s", err, want)
	}
}
