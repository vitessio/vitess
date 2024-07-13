/*
Copyright 2023 The Vitess Authors.

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

package dml

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	mapsx "golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/test/endtoend/utils"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type testQuery struct {
	tableName string
	cols      []string
	intTyp    []bool
}

var deleteUser, deleteUserExtra = "delete from user", "delete from user_extra"

func generateInserts(userSize int, userExtraSize int) (string, string) {
	var userInserts []string
	var userExtraInserts []string

	// Generate user table inserts
	for i := 1; i <= userSize; i++ {
		id := i
		notShardingKey := i
		typeValue := i % 5            // Just an example for type values
		teamID := i%userExtraSize + 1 // To ensure team_id references user_extra id
		userInserts = append(userInserts, fmt.Sprintf("(%d, %d, %d, %d)", id, notShardingKey, typeValue, teamID))
	}

	// Generate user_extra table inserts
	for i := 1; i <= userExtraSize; i++ {
		id := i
		notShardingKey := i
		colValue := fmt.Sprintf("col_value_%d", i)
		userExtraInserts = append(userExtraInserts, fmt.Sprintf("(%d, %d, '%s')", id, notShardingKey, colValue))
	}

	userInsertStatement := fmt.Sprintf("INSERT INTO user (id, not_sharding_key, type, team_id) VALUES %s;", strings.Join(userInserts, ", "))
	userExtraInsertStatement := fmt.Sprintf("INSERT INTO user_extra (id, not_sharding_key, col) VALUES %s;", strings.Join(userExtraInserts, ", "))

	return userInsertStatement, userExtraInsertStatement
}

func (tq *testQuery) getInsertQuery(rows int) string {
	var allRows []string
	for i := 0; i < rows; i++ {
		var row []string
		for _, isInt := range tq.intTyp {
			if isInt {
				row = append(row, strconv.Itoa(i))
				continue
			}
			row = append(row, "'"+getRandomString(50)+"'")
		}
		allRows = append(allRows, "("+strings.Join(row, ",")+")")
	}
	return fmt.Sprintf("insert into %s(%s) values %s", tq.tableName, strings.Join(tq.cols, ","), strings.Join(allRows, ","))
}

func (tq *testQuery) getUpdateQuery(rows int) string {
	var allRows []string
	var row []string
	for i, isInt := range tq.intTyp {
		if isInt {
			row = append(row, strconv.Itoa(i))
			continue
		}
		row = append(row, tq.cols[i]+" = '"+getRandomString(50)+"'")
	}
	allRows = append(allRows, strings.Join(row, ","))

	var ids []string
	for i := 0; i <= rows; i++ {
		ids = append(ids, strconv.Itoa(i))
	}
	return fmt.Sprintf("update %s set %s where id in (%s)", tq.tableName, strings.Join(allRows, ","), strings.Join(ids, ","))
}

func (tq *testQuery) getDeleteQuery(rows int) string {
	var ids []string
	for i := 0; i <= rows; i++ {
		ids = append(ids, strconv.Itoa(i))
	}
	return fmt.Sprintf("delete from %s where id in (%s)", tq.tableName, strings.Join(ids, ","))
}

func getRandomString(size int) string {
	var str strings.Builder

	for i := 0; i < size; i++ {
		str.WriteByte(byte(rand.IntN(27) + 97))
	}
	return str.String()
}

func BenchmarkShardedTblNoLookup(b *testing.B) {
	conn, closer := start(b)
	defer closer()

	cols := []string{"id", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12"}
	intType := make([]bool, len(cols))
	intType[0] = true
	tq := &testQuery{
		tableName: "tbl_no_lkp_vdx",
		cols:      cols,
		intTyp:    intType,
	}
	for _, rows := range []int{1, 10, 100, 500, 1000, 5000, 10000} {
		insStmt := tq.getInsertQuery(rows)
		b.Run(fmt.Sprintf("4-shards-%d-rows", rows), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = utils.Exec(b, conn, insStmt)
			}
		})
	}
}

func BenchmarkShardedTblUpdateIn(b *testing.B) {
	conn, closer := start(b)
	defer closer()

	cols := []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12"}
	intType := make([]bool, len(cols))
	tq := &testQuery{
		tableName: "tbl_no_lkp_vdx",
		cols:      cols,
		intTyp:    intType,
	}
	insStmt := tq.getInsertQuery(10000)
	_ = utils.Exec(b, conn, insStmt)
	for _, rows := range []int{1, 10, 100, 500, 1000, 5000, 10000} {
		updStmt := tq.getUpdateQuery(rows)
		b.Run(fmt.Sprintf("4-shards-%d-rows", rows), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = utils.Exec(b, conn, updStmt)
			}
		})
	}
}

func BenchmarkShardedTblDeleteIn(b *testing.B) {
	conn, closer := start(b)
	defer closer()
	tq := &testQuery{
		tableName: "tbl_no_lkp_vdx",
	}
	for _, rows := range []int{1, 10, 100, 500, 1000, 5000, 10000} {
		insStmt := tq.getInsertQuery(rows)
		_ = utils.Exec(b, conn, insStmt)
		delStmt := tq.getDeleteQuery(rows)
		b.Run(fmt.Sprintf("4-shards-%d-rows", rows), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = utils.Exec(b, conn, delStmt)
			}
		})
	}
}

func BenchmarkShardedAggrPushDown(b *testing.B) {
	conn, closer := start(b)
	defer closer()

	sizes := []int{100, 500, 1000}

	for _, user := range sizes {
		for _, userExtra := range sizes {
			insert1, insert2 := generateInserts(user, userExtra)
			_ = utils.Exec(b, conn, insert1)
			_ = utils.Exec(b, conn, insert2)
			b.Run(fmt.Sprintf("user-%d-user_extra-%d", user, userExtra), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_ = utils.Exec(b, conn, "select sum(user.type) from user join user_extra on user.team_id = user_extra.id group by user_extra.id order by user_extra.id")
				}
			})
			_ = utils.Exec(b, conn, deleteUser)
			_ = utils.Exec(b, conn, deleteUserExtra)
		}
	}
}

var mirrorInitOnce sync.Once

func BenchmarkMirror(b *testing.B) {
	const numRows = 10000

	conn, closer := start(b)
	defer closer()

	// Each time this BenchmarkMirror runs, use a different source of
	// randomness. But use the same source of randomness across test cases and
	// mirror percentages sub test cases.
	pcg := rand.NewPCG(rand.Uint64(), rand.Uint64())

	ksTables := map[string]string{
		sKs2: "mirror_tbl1",
		sKs3: "mirror_tbl2",
	}
	targetKeyspaces := mapsx.Keys(ksTables)

	mirrorInitOnce.Do(func() {
		b.Logf("seeding database for benchmark...")

		for i := 0; i < numRows; i++ {
			_, err := conn.ExecuteFetch(
				fmt.Sprintf("INSERT INTO %s.mirror_tbl1(id) VALUES(%d)", sKs1, i), -1, false)
			require.NoError(b, err)

			_, err = conn.ExecuteFetch(
				fmt.Sprintf("INSERT INTO %s.mirror_tbl2(id) VALUES(%d)", sKs1, i), -1, false)
			require.NoError(b, err)
		}

		_, err := conn.ExecuteFetch(
			fmt.Sprintf("SELECT COUNT(id) FROM %s.%s", sKs1, "mirror_tbl1"), 1, false)
		require.NoError(b, err)

		b.Logf("finished (inserted %d rows)", numRows)

		b.Logf("using MoveTables to copy data from source keyspace to target keyspaces")

		// Set up MoveTables workflows, which is (at present) the only way to set up
		// mirror rules.
		for tks, tbl := range ksTables {
			output, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
				"MoveTables", "--target-keyspace", tks, "--workflow", fmt.Sprintf("%s2%s", sKs1, tks),
				"create", "--source-keyspace", sKs1, "--tables", tbl)
			require.NoError(b, err, output)
		}

		// Wait for tables to be copied from source to targets.
		pending := make(map[string]string, len(ksTables))
		maps.Copy(pending, ksTables)
		for len(pending) > 0 {
			for tks := range ksTables {
				output, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
					"Workflow", "--keyspace", tks, "show", "--workflow", fmt.Sprintf("%s2%s", sKs1, tks))
				require.NoError(b, err, output)

				var response vtctldatapb.GetWorkflowsResponse
				require.NoError(b, protojson.Unmarshal([]byte(output), &response))

				require.Len(b, response.Workflows, 1)
				workflow := response.Workflows[0]

				require.Len(b, workflow.ShardStreams, 4 /*shards*/)
				for _, ss := range workflow.ShardStreams {
					for _, s := range ss.Streams {
						if s.State == "Running" {
							delete(pending, tks)
						} else {
							b.Logf("waiting for workflow %s.%s stream %s=>%s to be running; last state: %s",
								workflow.Target, workflow.Name, s.BinlogSource.Shard, s.Shard, s.State)
							time.Sleep(1 * time.Second)
						}
					}
				}
			}
		}
	})

	testCases := []struct {
		name string
		run  func(*testing.B, *rand.Rand)
	}{
		{
			name: "point select, { sks1 => sks2 }.mirror_tbl1",
			run: func(b *testing.B, rnd *rand.Rand) {
				for i := 0; i < b.N; i++ {
					id := rnd.Int32N(numRows)
					_, err := conn.ExecuteFetch(fmt.Sprintf(
						"SELECT t1.id FROM %s.mirror_tbl1 AS t1 WHERE t1.id = %d",
						sKs1, id,
					), 1, false)
					if err != nil {
						b.Error(err)
					}
				}
			},
		},
		{
			name: "point select, { sks1 => sks2 }.mirror_tbl1, { sks1 => sks3 }.mirror_tbl2",
			run: func(b *testing.B, rnd *rand.Rand) {
				for i := 0; i < b.N; i++ {
					id := rnd.Int32N(numRows)
					_, err := conn.ExecuteFetch(fmt.Sprintf(
						"SELECT t1.id, t2.id FROM %s.mirror_tbl1 AS t1, %s.mirror_tbl2 AS t2 WHERE t1.id = %d AND t2.id = %d",
						sKs1, sKs1, id, id,
					), 1, false)
					if err != nil {
						b.Error(err)
					}
				}
			},
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("mirror 0%", func(b *testing.B) {
				mirrorTraffic(b, targetKeyspaces, 0)
				b.ResetTimer()
				tc.run(b, rand.New(pcg))
			})

			b.Run("mirror 1%", func(b *testing.B) {
				mirrorTraffic(b, targetKeyspaces, 1)
				b.ResetTimer()
				tc.run(b, rand.New(pcg))
			})

			b.Run("mirror 5%", func(b *testing.B) {
				mirrorTraffic(b, targetKeyspaces, 5)
				b.ResetTimer()
				tc.run(b, rand.New(pcg))
			})

			b.Run("mirror 10%", func(b *testing.B) {
				mirrorTraffic(b, targetKeyspaces, 10)
				b.ResetTimer()
				tc.run(b, rand.New(pcg))
			})

			b.Run("mirror 25%", func(b *testing.B) {
				mirrorTraffic(b, targetKeyspaces, 25)
				b.ResetTimer()
				tc.run(b, rand.New(pcg))
			})

			b.Run("mirror 50%", func(b *testing.B) {
				mirrorTraffic(b, targetKeyspaces, 50)
				b.ResetTimer()
				tc.run(b, rand.New(pcg))
			})

			b.Run("mirror 100%", func(b *testing.B) {
				mirrorTraffic(b, targetKeyspaces, 100)
				b.ResetTimer()
				tc.run(b, rand.New(pcg))
			})
		})
	}
}

func mirrorTraffic(b *testing.B, targetKeyspaces []string, percent float32) {
	for _, tks := range targetKeyspaces {
		output, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
			"MoveTables", "--target-keyspace", tks, "--workflow", fmt.Sprintf("%s2%s", sKs1, tks),
			"mirrortraffic", "--percent", fmt.Sprintf("%.02f", percent))
		require.NoError(b, err, output)
	}
}
