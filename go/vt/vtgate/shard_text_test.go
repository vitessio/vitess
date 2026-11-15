/*
Copyright 2026 The Vitess Authors.

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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// shardTextResult is a result with a latin1_german2_ci VARCHAR(10) with
// 'Müller'. With raw it is as a tablet with
// --queryserver-config-raw-result-charset returns it, 4D FC 6C 6C 65 72 with
// the collation 31, and without it as a tablet without the flag does, 4D C3 BC
// 6C 6C 65 72 with the collation 255.
func shardTextResult(raw bool) *sqltypes.Result {
	if raw {
		return &sqltypes.Result{
			Fields: []*querypb.Field{{Name: "textcol", Type: sqltypes.VarChar, Charset: 31, ColumnLength: 10}},
			Rows:   []sqltypes.Row{{sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xFCller"))}},
		}
	}
	return &sqltypes.Result{
		Fields: []*querypb.Field{{Name: "textcol", Type: sqltypes.VarChar, Charset: 255, ColumnLength: 40}},
		Rows:   []sqltypes.Row{{sqltypes.NewVarChar("Müller")}},
	}
}

// TestShardText runs a statement on two shards of which one tablet sends the
// text of a latin1 column in latin1 and the other in utf8mb4. With
// --convert-result-charset the rows of both are in the character set that the
// fields of the result say, whichever shard answered first: two rows of 4D FC
// 6C 6C 65 72 with the collation 31, or two of 4D C3 BC 6C 6C 65 72 with the
// collation 255. Without the flag the rows are as the tablets sent them, one
// of each, as before.
func TestShardText(t *testing.T) {
	const keyspace = "TestShardText"
	for _, convert := range []bool{true, false} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("convert %v, stream %v", convert, stream), func(t *testing.T) {
				setConvertResultCharset(t, convert)
				ctx := utils.LeakCheckContext(t)
				hc := discovery.NewFakeHealthCheck(nil)
				createSandbox(keyspace).Reset()
				sc := newTestScatterConn(ctx, hc, newSandboxForCells(ctx, []string{"aa"}), "aa")
				sbc0 := hc.AddTestTablet("aa", "0", 1, keyspace, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
				sbc1 := hc.AddTestTablet("aa", "1", 1, keyspace, "1", topodatapb.TabletType_REPLICA, true, 1, nil)
				raw, converted := shardTextResult(true), shardTextResult(false)
				sbc0.SetResults([]*sqltypes.Result{raw})
				sbc1.SetResults([]*sqltypes.Result{converted})

				res := srvtopo.NewResolver(newSandboxForCells(ctx, []string{"aa"}), sc.gateway, "aa")
				rss, err := res.ResolveDestination(ctx, keyspace, topodatapb.TabletType_REPLICA, key.DestinationShards([]string{"0", "1"}))
				require.NoError(t, err)

				qr := new(sqltypes.Result)
				var errs []error
				if stream {
					var mu sync.Mutex
					bvs := make([]map[string]*querypb.BindVariable, len(rss))
					errs = sc.StreamExecuteMulti(ctx, nil, "query", rss, bvs, econtext.NewSafeSession(nil), true /* autocommit */, func(r *sqltypes.Result) error {
						mu.Lock()
						defer mu.Unlock()
						qr.AppendResult(r)
						return nil
					}, nullResultsObserver{}, false)
				} else {
					queries := []*querypb.BoundQuery{{Sql: "query"}, {Sql: "query"}}
					qr, errs = sc.ExecuteMultiShard(ctx, nil, rss, queries, econtext.NewSafeSession(nil), false /*autocommit*/, false, nullResultsObserver{}, false)
				}
				require.NoError(t, vterrors.Aggregate(errs))
				require.Len(t, qr.Fields, 1)

				counts := map[string]int{}
				for _, row := range qr.Rows {
					counts[row[0].ToString()]++
				}
				switch {
				case !convert:
					assert.Equal(t, map[string]int{"M\xFCller": 1, "Müller": 1}, counts)
				case qr.Fields[0].Charset == 31:
					assert.Equal(t, map[string]int{"M\xFCller": 2}, counts)
				default:
					assert.Equal(t, uint32(255), qr.Fields[0].Charset)
					assert.Equal(t, map[string]int{"Müller": 2}, counts)
				}

				// The results of the tablets are not changed.
				assert.Equal(t, shardTextResult(true), raw)
				assert.Equal(t, shardTextResult(false), converted)
			})
		}
	}
}
