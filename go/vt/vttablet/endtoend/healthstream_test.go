/*
Copyright 2021 The Vitess Authors.

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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

func TestSchemaChange(t *testing.T) {
	client := framework.NewClient()

	tcs := []struct {
		tName    string
		response []string
		ddl      string
	}{
		{
			"default",
			[]string{"upsert_test", "vitess_a", "vitess_acl_admin", "vitess_acl_all_user_read_only", "vitess_acl_no_access", "vitess_acl_read_only", "vitess_acl_read_write", "vitess_acl_unmatched", "vitess_autoinc_seq", "vitess_b", "vitess_big", "vitess_bit_default", "vitess_bool", "vitess_c", "vitess_d", "vitess_e", "vitess_f", "vitess_fracts", "vitess_ints", "vitess_misc", "vitess_mixed_case", "vitess_part", "vitess_reset_seq", "vitess_seq", "vitess_stress", "vitess_strings", "vitess_test", "vitess_test_debuguser"},
			"",
		}, {
			"create table 1",
			[]string{"vitess_sc1"},
			"create table vitess_sc1(id bigint primary key)",
		}, {
			"create table 2",
			[]string{"vitess_sc2"},
			"create table vitess_sc2(id bigint primary key)",
		}, {
			"add column 1",
			[]string{"vitess_sc1"},
			"alter table vitess_sc1 add column newCol varchar(50)",
		}, {
			"add column 2",
			[]string{"vitess_sc2"},
			"alter table vitess_sc2 add column newCol varchar(50)",
		}, {
			"remove column",
			[]string{"vitess_sc1"},
			"alter table vitess_sc1 drop column newCol",
		}, {
			"drop table",
			[]string{"vitess_sc2"},
			"drop table vitess_sc2",
		},
	}

	ch := make(chan []string)
	var wg sync.WaitGroup
	wg.Add(1)
	go func(ch chan []string) {
		client.StreamHealth(func(response *querypb.StreamHealthResponse) error {
			if response.RealtimeStats.TableSchemaChanged != nil {
				ch <- response.RealtimeStats.TableSchemaChanged
			}
			return nil
		})
	}(ch)

	go func(client *framework.QueryClient, ch chan []string) {
		index := 0
		for {
			t.Run(strconv.Itoa(index), func(t *testing.T) {
				res := <-ch
				utils.MustMatch(t, tcs[index].response, res, "")
			})
			index++
			if index == len(tcs) {
				close(ch)
				break
			}
			if tcs[index].ddl != "" {
				_, err := client.Execute(tcs[index].ddl, nil)
				require.NoError(t, err)
			}
		}
		wg.Done()
	}(client, ch)

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
	case <-time.After(5 * time.Second):
		t.Errorf("timed out")
	}

}
