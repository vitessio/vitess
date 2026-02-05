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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

func TestSchemaChange(t *testing.T) {
	client := framework.NewClient()

	tcs := []struct {
		tName          string
		expectedChange string
		ddl            string
		expectTimeout  bool
	}{
		{
			"create table 1",
			"vitess_sc1",
			"create table vitess_sc1(id bigint primary key)",
			false,
		}, {
			"create table 2",
			"vitess_sc2",
			"create table vitess_sc2(id bigint primary key)",
			false,
		}, {
			"create internal table",
			"_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410",
			"create table _vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410(id bigint primary key)",
			true,
		}, {
			"add column 1",
			"vitess_sc1",
			"alter table vitess_sc1 add column newCol varchar(50)",
			false,
		}, {
			"add column 2",
			"vitess_sc2",
			"alter table vitess_sc2 add column newCol varchar(50)",
			false,
		}, {
			"remove column",
			"vitess_sc1",
			"alter table vitess_sc1 drop column newCol",
			false,
		}, {
			"drop table 2",
			"vitess_sc2",
			"drop table vitess_sc2",
			false,
		}, {
			"drop table 1",
			"vitess_sc1",
			"drop table vitess_sc1",
			false,
		},
	}

	ch := make(chan []string, 100)
	go func(ch chan []string) {
		client.StreamHealth(func(response *querypb.StreamHealthResponse) error {
			if response.RealtimeStats.TableSchemaChanged != nil {
				ch <- response.RealtimeStats.TableSchemaChanged
			}
			return nil
		})
	}(ch)

	for _, tc := range tcs {
		t.Run(tc.tName, func(t *testing.T) {
			_, err := client.Execute(tc.ddl, nil)
			assert.NoError(t, err)
			timeout := time.After(5 * time.Second)
			for {
				select {
				case res := <-ch: // get the schema notification
					if slices.Contains(res, tc.expectedChange) {
						assert.False(t, tc.expectTimeout)
						return
					}
				case <-timeout:
					if tc.expectTimeout {
						// This is what we wanted!
						return
					}
					t.Errorf("timed out waiting for a schema notification")
					return
				}
			}
		})
	}
}
