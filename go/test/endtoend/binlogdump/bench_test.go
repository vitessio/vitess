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

package binlogdump

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
)

// benchCase defines a benchmark scenario with a name and an insert function
// that populates test data and returns the GTID range bracketing the inserts.
type benchCase struct {
	name   string
	insert func(b *testing.B, conn *mysql.Conn) (startGTID, endGTID string)
}

// BenchmarkBinlogDumpThroughput measures binlog dump throughput via three paths
// to isolate where overhead is introduced:
//
//  1. Direct  — client → MySQL unix socket (baseline)
//  2. gRPC    — client → vttablet gRPC (adds: streamBinlogPackets buffering + protobuf + gRPC)
//  3. VTGate  — client → VTGate MySQL port → vttablet gRPC (adds: VTGate routing + packet reassembly)
//
// Two row-size scenarios are tested:
//   - SmallRows: 100K small rows (~2 MB) — exercises the non-spanning packet path
//   - LargeRows: 100 rows with 512KB blobs (~50 MB) — each binlog event exceeds
//     the 256KB gRPC response buffer, exercising the spanning-packet path
//
// All use nonBlock mode so the stream terminates with EOF after catching up.
func BenchmarkBinlogDumpThroughput(b *testing.B) {
	cases := []benchCase{
		{
			name: "SmallRows",
			insert: func(b *testing.B, conn *mysql.Conn) (string, string) {
				b.Helper()

				qr, err := conn.ExecuteFetch("SELECT @@global.gtid_executed", 1, false)
				require.NoError(b, err)
				require.Len(b, qr.Rows, 1)
				startGTID := qr.Rows[0][0].ToString()

				const numRows = 100_000
				const batchSize = 1000
				for i := range numRows / batchSize {
					var query string
					for j := range batchSize {
						row := i*batchSize + j
						if j == 0 {
							query = fmt.Sprintf("INSERT INTO binlog_test (msg) VALUES ('bench_%d')", row)
						} else {
							query += fmt.Sprintf(",('bench_%d')", row)
						}
					}
					_, err := conn.ExecuteFetch(query, 0, false)
					require.NoError(b, err)
				}

				qr, err = conn.ExecuteFetch("SELECT @@global.gtid_executed", 1, false)
				require.NoError(b, err)
				require.Len(b, qr.Rows, 1)
				endGTID := qr.Rows[0][0].ToString()

				b.Logf("SmallRows: inserted %d rows in batches of %d", numRows, batchSize)
				return startGTID, endGTID
			},
		},
		{
			name: "LargeRows",
			insert: func(b *testing.B, conn *mysql.Conn) (string, string) {
				b.Helper()

				qr, err := conn.ExecuteFetch("SELECT @@global.gtid_executed", 1, false)
				require.NoError(b, err)
				require.Len(b, qr.Rows, 1)
				startGTID := qr.Rows[0][0].ToString()

				const numRows = 100
				for i := range numRows {
					_, err := conn.ExecuteFetch(
						fmt.Sprintf("INSERT INTO large_blob_test (data) VALUES (REPEAT('x', 524288))/* row %d */", i), 0, false)
					require.NoError(b, err)
				}

				qr, err = conn.ExecuteFetch("SELECT @@global.gtid_executed", 1, false)
				require.NoError(b, err)
				require.Len(b, qr.Rows, 1)
				endGTID := qr.Rows[0][0].ToString()

				b.Logf("LargeRows: inserted %d rows with 512KB blobs", numRows)
				return startGTID, endGTID
			},
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()

			dataConn, err := mysql.Connect(ctx, &vtParams)
			require.NoError(b, err)
			defer dataConn.Close()

			startGTID, endGTID := tc.insert(b, dataConn)
			b.Logf("Start GTID: %s", startGTID)
			b.Logf("End GTID: %s", endGTID)

			startGTIDSet, err := replication.ParseMysql56GTIDSet(startGTID)
			require.NoError(b, err)
			sidBlock := startGTIDSet.SIDBlock()

			flags := uint16(mysql.BinlogDumpNonBlock | mysql.BinlogThroughGTID)

			// Helper: dump from startGTID to EOF via MySQL protocol, return total bytes and packet count.
			dumpToEOF := func(b *testing.B, conn *mysql.Conn, setBinlogChecksum bool) (totalBytes int64, packets int) {
				b.Helper()

				if setBinlogChecksum {
					_, err := conn.ExecuteFetch("SET @source_binlog_checksum = @@global.binlog_checksum, @master_binlog_checksum = @@global.binlog_checksum", 0, false)
					require.NoError(b, err)
				}

				err := conn.WriteComBinlogDumpGTID(1, "", 4, flags, sidBlock)
				require.NoError(b, err)

				timeout := time.After(60 * time.Second)
				for {
					select {
					case <-timeout:
						b.Fatalf("Timeout after %d packets (%d bytes)", packets, totalBytes)
					default:
					}

					data, err := conn.ReadPacket()
					if err != nil {
						b.Fatalf("ReadPacket error after %d packets: %v", packets, err)
					}
					if len(data) == 0 {
						continue
					}

					switch data[0] {
					case mysql.EOFPacket:
						return totalBytes, packets
					case mysql.ErrPacket:
						sqlErr := mysql.ParseErrorPacket(data)
						b.Fatalf("Error packet after %d packets: %v", packets, sqlErr)
					default:
						packets++
						totalBytes += int64(len(data))
					}
				}
			}

			// --- Sub-benchmark: Direct MySQL (unix socket) ---
			b.Run("Direct", func(b *testing.B) {
				primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
				socketPath := path.Join(os.Getenv("VTDATAROOT"),
					fmt.Sprintf("vt_%010d", primaryTablet.TabletUID), "mysql.sock")

				directParams := mysql.ConnParams{
					Uname:      "vt_dba",
					UnixSocket: socketPath,
				}

				// Warmup iteration to get byte count for SetBytes.
				warmConn, err := mysql.Connect(ctx, &directParams)
				require.NoError(b, err)
				warmBytes, warmPkts := dumpToEOF(b, warmConn, true)
				warmConn.Close()
				b.Logf("Warmup: %d packets, %d bytes (%.1f MB)", warmPkts, warmBytes, float64(warmBytes)/(1024*1024))
				b.SetBytes(warmBytes)

				b.ResetTimer()
				for range b.N {
					conn, err := mysql.Connect(ctx, &directParams)
					require.NoError(b, err)
					dumpToEOF(b, conn, true)
					conn.Close()
				}
			})

			// --- Sub-benchmark: VTGate (full path) ---
			b.Run("VTGate", func(b *testing.B) {
				primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
				targetString := fmt.Sprintf("%s:0@primary|%s", keyspaceName, primaryTablet.Alias)
				vtgateParams := mysql.ConnParams{
					Host:  clusterInstance.Hostname,
					Port:  clusterInstance.VtgateMySQLPort,
					Uname: "vt_repl|" + targetString,
				}

				// Warmup iteration to get byte count for SetBytes.
				warmConn, err := mysql.Connect(ctx, &vtgateParams)
				require.NoError(b, err)
				warmBytes, warmPkts := dumpToEOF(b, warmConn, false)
				warmConn.Close()
				b.Logf("Warmup: %d packets, %d bytes (%.1f MB)", warmPkts, warmBytes, float64(warmBytes)/(1024*1024))
				b.SetBytes(warmBytes)

				b.ResetTimer()
				for range b.N {
					conn, err := mysql.Connect(ctx, &vtgateParams)
					require.NoError(b, err)
					dumpToEOF(b, conn, false)
					conn.Close()
				}
			})
		})
	}
}
