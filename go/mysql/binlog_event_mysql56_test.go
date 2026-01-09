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

package mysql

import (
	_ "embed"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
)

// Sample event data for MySQL 5.6.
var (
	mysql56FormatEvent             = NewMysql56BinlogEvent([]byte{0x78, 0x4e, 0x49, 0x55, 0xf, 0x64, 0x0, 0x0, 0x0, 0x74, 0x0, 0x0, 0x0, 0x78, 0x0, 0x0, 0x0, 0x1, 0x0, 0x4, 0x0, 0x35, 0x2e, 0x36, 0x2e, 0x32, 0x34, 0x2d, 0x6c, 0x6f, 0x67, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x78, 0x4e, 0x49, 0x55, 0x13, 0x38, 0xd, 0x0, 0x8, 0x0, 0x12, 0x0, 0x4, 0x4, 0x4, 0x4, 0x12, 0x0, 0x0, 0x5c, 0x0, 0x4, 0x1a, 0x8, 0x0, 0x0, 0x0, 0x8, 0x8, 0x8, 0x2, 0x0, 0x0, 0x0, 0xa, 0xa, 0xa, 0x19, 0x19, 0x0, 0x1, 0x18, 0x4a, 0xf, 0xca})
	mysql56GTIDEvent               = NewMysql56BinlogEvent([]byte{0xff, 0x4e, 0x49, 0x55, 0x21, 0x64, 0x0, 0x0, 0x0, 0x30, 0x0, 0x0, 0x0, 0xf5, 0x2, 0x0, 0x0, 0x0, 0x0, 0x1, 0x43, 0x91, 0x92, 0xbd, 0xf3, 0x7c, 0x11, 0xe4, 0xbb, 0xeb, 0x2, 0x42, 0xac, 0x11, 0x3, 0x5a, 0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0 /* lt_type: */, 0x0 /* last_committed: */, 0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0 /* sequence_number: */, 0x9, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x48, 0x45, 0x82, 0x27})
	mysql56QueryEvent              = NewMysql56BinlogEvent([]byte{0xff, 0x4e, 0x49, 0x55, 0x2, 0x64, 0x0, 0x0, 0x0, 0x77, 0x0, 0x0, 0x0, 0xdb, 0x3, 0x0, 0x0, 0x0, 0x0, 0x3d, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x21, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x3, 0x73, 0x74, 0x64, 0x4, 0x8, 0x0, 0x8, 0x0, 0x21, 0x0, 0xc, 0x1, 0x74, 0x65, 0x73, 0x74, 0x0, 0x74, 0x65, 0x73, 0x74, 0x0, 0x69, 0x6e, 0x73, 0x65, 0x72, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x6f, 0x20, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x28, 0x6d, 0x73, 0x67, 0x29, 0x20, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x20, 0x28, 0x27, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x27, 0x29, 0x92, 0x12, 0x79, 0xc3})
	mysql56SemiSyncNoAckQueryEvent = NewMysql56BinlogEvent([]byte{0xef, 0x00, 0xff, 0x4e, 0x49, 0x55, 0x2, 0x64, 0x0, 0x0, 0x0, 0x77, 0x0, 0x0, 0x0, 0xdb, 0x3, 0x0, 0x0, 0x0, 0x0, 0x3d, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x21, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x3, 0x73, 0x74, 0x64, 0x4, 0x8, 0x0, 0x8, 0x0, 0x21, 0x0, 0xc, 0x1, 0x74, 0x65, 0x73, 0x74, 0x0, 0x74, 0x65, 0x73, 0x74, 0x0, 0x69, 0x6e, 0x73, 0x65, 0x72, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x6f, 0x20, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x28, 0x6d, 0x73, 0x67, 0x29, 0x20, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x20, 0x28, 0x27, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x27, 0x29, 0x92, 0x12, 0x79, 0xc3})
	mysql56SemiSyncAckQueryEvent   = NewMysql56BinlogEvent([]byte{0xef, 0x01, 0xff, 0x4e, 0x49, 0x55, 0x2, 0x64, 0x0, 0x0, 0x0, 0x77, 0x0, 0x0, 0x0, 0xdb, 0x3, 0x0, 0x0, 0x0, 0x0, 0x3d, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x21, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x3, 0x73, 0x74, 0x64, 0x4, 0x8, 0x0, 0x8, 0x0, 0x21, 0x0, 0xc, 0x1, 0x74, 0x65, 0x73, 0x74, 0x0, 0x74, 0x65, 0x73, 0x74, 0x0, 0x69, 0x6e, 0x73, 0x65, 0x72, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x6f, 0x20, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x28, 0x6d, 0x73, 0x67, 0x29, 0x20, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x20, 0x28, 0x27, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x27, 0x29, 0x92, 0x12, 0x79, 0xc3})
)

//go:embed large_compressed_trx_payload.bin
var mysql56CompressedLargeTrxPayload []byte

func TestMysql56IsGTID(t *testing.T) {
	if got, want := mysql56FormatEvent.IsGTID(), false; got != want {
		t.Errorf("%#v.IsGTID() = %#v, want %#v", mysql56FormatEvent, got, want)
	}
	if got, want := mysql56QueryEvent.IsGTID(), false; got != want {
		t.Errorf("%#v.IsGTID() = %#v, want %#v", mysql56QueryEvent, got, want)
	}
	if got, want := mysql56GTIDEvent.IsGTID(), true; got != want {
		t.Errorf("%#v.IsGTID() = %#v, want %#v", mysql56GTIDEvent, got, want)
	}
}

func TestMysql56StripChecksum(t *testing.T) {
	format, err := mysql56FormatEvent.Format()
	require.NoError(t, err, "Format() error: %v", err)

	stripped, gotChecksum, err := mysql56QueryEvent.StripChecksum(format)
	require.NoError(t, err, "StripChecksum() error: %v", err)

	// Check checksum.
	if want := []byte{0x92, 0x12, 0x79, 0xc3}; !reflect.DeepEqual(gotChecksum, want) {
		t.Errorf("checksum = %#v, want %#v", gotChecksum, want)
	}

	// Check query, to make sure checksum was stripped properly.
	// Query length is defined as "the rest of the bytes after offset X",
	// so the query will be wrong if the checksum is not stripped.
	gotQuery, err := stripped.Query(format)
	require.NoError(t, err, "Query() error: %v", err)

	if want := "insert into test_table (msg) values ('hello')"; string(gotQuery.SQL) != want {
		t.Errorf("query = %#v, want %#v", string(gotQuery.SQL), want)
	}
}

func TestMysql56GTID(t *testing.T) {
	format, err := mysql56FormatEvent.Format()
	require.NoError(t, err, "Format() error: %v", err)

	input, _, err := mysql56GTIDEvent.StripChecksum(format)
	require.NoError(t, err, "StripChecksum() error: %v", err)
	require.True(t, input.IsGTID(), "IsGTID() = false, want true")

	want := replication.Mysql56GTID{
		Server:   replication.SID{0x43, 0x91, 0x92, 0xbd, 0xf3, 0x7c, 0x11, 0xe4, 0xbb, 0xeb, 0x2, 0x42, 0xac, 0x11, 0x3, 0x5a},
		Sequence: 4,
	}
	got, hasBegin, lastCommitted, sequenceNumber, err := input.GTID(format)
	require.NoError(t, err, "GTID() error: %v", err)
	assert.False(t, hasBegin, "GTID() returned hasBegin")
	assert.Equal(t, want, got, "GTID() = %#v, want %#v", got, want)
	assert.Equal(t, int64(7), lastCommitted)
	assert.Equal(t, int64(9), sequenceNumber)
}

func TestMysql56DecodeTransactionPayload(t *testing.T) {
	format := NewMySQL56BinlogFormat()
	tableMap := &TableMap{}

	testCases := []struct {
		name     string
		event    BinlogEvent
		want     []string
		inMemory bool
	}{
		{
			name:  "Small event done in memory",
			event: NewMysql56BinlogEvent([]byte{0xc7, 0xe1, 0x4b, 0x64, 0x28, 0x5b, 0xd2, 0xc7, 0x19, 0xdb, 0x00, 0x00, 0x00, 0x3a, 0x50, 0x00, 0x00, 0x00, 0x00, 0x02, 0x01, 0x00, 0x03, 0x03, 0xfc, 0xfe, 0x00, 0x01, 0x01, 0xb8, 0x00, 0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58, 0x64, 0x05, 0x00, 0xf2, 0x49, 0x23, 0x2a, 0xa0, 0x27, 0x69, 0x0c, 0xff, 0xe8, 0x06, 0xeb, 0xfe, 0xc3, 0xab, 0x8a, 0x7b, 0xc0, 0x36, 0x42, 0x5c, 0x6f, 0x1b, 0x2f, 0xfb, 0x6e, 0xc4, 0x9a, 0xe6, 0x6e, 0x6b, 0xda, 0x08, 0xf1, 0x37, 0x7e, 0xff, 0xb8, 0x6c, 0xbc, 0x27, 0x3c, 0xb7, 0x4f, 0xee, 0x14, 0xff, 0xaf, 0x09, 0x06, 0x69, 0xe3, 0x12, 0x68, 0x4a, 0x6e, 0xc3, 0xe1, 0x28, 0xaf, 0x3f, 0xc8, 0x14, 0x1c, 0xc3, 0x60, 0xce, 0xe3, 0x1e, 0x18, 0x4c, 0x63, 0xa1, 0x35, 0x90, 0x79, 0x04, 0xe8, 0xa9, 0xeb, 0x4a, 0x1b, 0xd7, 0x41, 0x53, 0x72, 0x17, 0xa4, 0x23, 0xa4, 0x47, 0x68, 0x00, 0xa2, 0x37, 0xee, 0xc1, 0xc7, 0x71, 0x30, 0x24, 0x19, 0xfd, 0x78, 0x49, 0x1b, 0x97, 0xd2, 0x94, 0xdc, 0x85, 0xa2, 0x21, 0xc1, 0xb0, 0x63, 0x8d, 0x7b, 0x0f, 0x32, 0x87, 0x07, 0xe2, 0x39, 0xf0, 0x7c, 0x3e, 0x01, 0xfe, 0x13, 0x8f, 0x11, 0xd0, 0x05, 0x9f, 0xbc, 0x18, 0x59, 0x91, 0x36, 0x2e, 0x6d, 0x4a, 0x6e, 0x0b, 0x00, 0x5e, 0x28, 0x10, 0xc0, 0x02, 0x50, 0x77, 0xe0, 0x64, 0x30, 0x02, 0x9e, 0x09, 0x54, 0xec, 0x80, 0x6d, 0x07, 0xa4, 0xc1, 0x7d, 0x60, 0xe4, 0x01, 0x78, 0x01, 0x01, 0x00, 0x00}),
			// The generated event is the result of the following SQL being executed in vtgate
			// against the commerce keyspace:
			// begin; insert into customer values (1, "mlord@planetscale.com"), (2, "sup@planetscale.com"); commit;
			// All of these below internal events are encoded in the compressed transaction
			// payload event.
			want: []string{
				"BEGIN",                     // Query event
				"vt_commerce.customer",      // TableMap event
				"[1 mlord@planetscale.com]", // WriteRows event
				"[2 sup@planetscale.com]",   // WriteRows event
				"COMMIT",                    // XID event
			},
			inMemory: true,
		},
		{
			name:  "Large event using streaming",
			event: NewMysql56BinlogEvent(mysql56CompressedLargeTrxPayload),
			// The generated event is the result of the following SQL being executed against the
			// commerce keyspace after having added a LONGTEXT column to the customer
			// table (this generates an uncompressed transaction that is over 128MiB):
			// insert into customer values (1, "mlord@planetscale.com", repeat("test", 43280000));
			// All of these below internal events are encoded in the compressed transaction
			// payload event.
			want: []string{
				"BEGIN",                // Query event
				"vt_commerce.customer", // TableMap event
				"[1 mlord@planetscale.com testtesttesttesttesttesttesttest", // WriteRows event
				"COMMIT", // XID event
			},
			inMemory: false,
		},
	}

	// Ensure that we can process events where the *uncompressed* size is
	// larger than ZstdInMemoryDecompressorMaxSize. The *compressed* size
	// of the payload in large_compressed_trx_payload.bin is 16KiB so we
	// set the max to 2KiB to test this.
	ZstdInMemoryDecompressorMaxSize = 2048

	for _, tc := range testCases {
		memDecodingCnt := compressedTrxPayloadsInMem.Get()
		streamDecodingCnt := compressedTrxPayloadsUsingStream.Get()

		require.True(t, tc.event.IsTransactionPayload())
		tp, err := tc.event.TransactionPayload(format)
		require.NoError(t, err)
		defer tp.Close()
		eventStrs := []string{}
		for {
			ev, err := tp.GetNextEvent()
			if err != nil {
				if err == io.EOF {
					break
				}
				require.Fail(t, fmt.Sprintf("unexpected error: %v", err))
			}
			switch {
			case ev.IsTableMap():
				tableMap, err = ev.TableMap(format)
				require.NoError(t, err)
				eventStrs = append(eventStrs, fmt.Sprintf("%s.%s", tableMap.Database, tableMap.Name))
			case ev.IsQuery():
				query, err := ev.Query(format)
				require.NoError(t, err)
				eventStrs = append(eventStrs, query.SQL)
			case ev.IsWriteRows() || ev.IsUpdateRows() || ev.IsDeleteRows():
				rows, err := ev.Rows(format, tableMap)
				require.NoError(t, err)
				for i := range rows.Rows {
					rowStr, err := rows.StringValuesForTests(tableMap, i)
					require.NoError(t, err)
					eventStrs = append(eventStrs, fmt.Sprintf("%v", rowStr))
				}
			case ev.IsXID():
				eventStrs = append(eventStrs, "COMMIT")
			}
		}
		if tc.inMemory {
			require.False(t, tp.StreamingContents)
			require.Equal(t, memDecodingCnt+1, compressedTrxPayloadsInMem.Get())
			require.Equal(t, tc.want, eventStrs)
		} else {
			require.True(t, tp.StreamingContents)
			require.Equal(t, streamDecodingCnt+1, compressedTrxPayloadsUsingStream.Get())
			require.Len(t, eventStrs, len(tc.want))
			totalSize := 0
			for i, want := range tc.want {
				eventStr := eventStrs[i]
				totalSize += len(eventStr)
				require.True(t, strings.HasPrefix(eventStr, want))
			}
			require.Greater(t, uint64(totalSize), ZstdInMemoryDecompressorMaxSize)
		}
	}
}

func TestMysql56ParsePosition(t *testing.T) {
	input := "00010203-0405-0607-0809-0a0b0c0d0e0f:1-2"

	sid := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	var set replication.GTIDSet = replication.Mysql56GTIDSet{}
	set = set.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 1})
	set = set.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 2})
	want := replication.Position{GTIDSet: set}

	got, err := replication.ParsePosition(replication.Mysql56FlavorID, input)
	assert.NoError(t, err, "unexpected error: %v", err)
	assert.True(t, got.Equal(want), "(&mysql56{}).ParsePosition(%#v) = %#v, want %#v", input, got, want)
}

func TestMysql56SemiSyncAck(t *testing.T) {
	{
		c := Conn{ExpectSemiSyncIndicator: false}
		buf, semiSyncAckRequested, err := c.AnalyzeSemiSyncAckRequest(mysql56QueryEvent.Bytes())
		assert.NoError(t, err)
		e := NewMysql56BinlogEventWithSemiSyncInfo(buf, semiSyncAckRequested)

		assert.False(t, e.IsSemiSyncAckRequested())
		assert.True(t, e.IsQuery())
	}
	{
		c := Conn{ExpectSemiSyncIndicator: true}
		buf, semiSyncAckRequested, err := c.AnalyzeSemiSyncAckRequest(mysql56SemiSyncNoAckQueryEvent.Bytes())
		assert.NoError(t, err)
		e := NewMysql56BinlogEventWithSemiSyncInfo(buf, semiSyncAckRequested)

		assert.False(t, e.IsSemiSyncAckRequested())
		assert.True(t, e.IsQuery())
	}
	{
		c := Conn{ExpectSemiSyncIndicator: true}
		buf, semiSyncAckRequested, err := c.AnalyzeSemiSyncAckRequest(mysql56SemiSyncAckQueryEvent.Bytes())
		assert.NoError(t, err)
		e := NewMysql56BinlogEventWithSemiSyncInfo(buf, semiSyncAckRequested)

		assert.True(t, e.IsSemiSyncAckRequested())
		assert.True(t, e.IsQuery())
	}
}

func TestMySQL56PartialUpdateRowsEvent(t *testing.T) {
	format := BinlogFormat{
		HeaderSizes: []byte{
			0, 13, 0, 8, 0, 0, 0, 0, 4, 0, 4, 0, 0, 0, 98, 0, 4, 26, 8, 0, 0, 0, 8, 8, 8, 2, 0, 0, 0, 10, 10, 10, 42, 42, 0, 18, 52, 0, 10, 40, 0,
		},
		ServerVersion:     "8.0.40",
		FormatVersion:     4,
		HeaderLength:      19,
		ChecksumAlgorithm: 1,
	}
	// This is from the following table structure:
	// CREATE TABLE `customer` (
	//   `customer_id` bigint NOT NULL AUTO_INCREMENT,
	//   `email` varbinary(128) DEFAULT NULL,
	//   `jd` json DEFAULT NULL,
	//   PRIMARY KEY (`customer_id`)
	// )
	tm := &TableMap{
		Flags:    1,
		Database: "vt_commerce",
		Name:     "customer",
		Types:    []byte{8, 15, 245},
		CanBeNull: Bitmap{
			data:  []byte{6},
			count: 3,
		},
		Metadata:           []uint16{0, 128, 4},
		ColumnCollationIDs: []collations.ID{63},
	}

	testCases := []struct {
		name     string
		rawEvent []byte
		numRows  int
		want     string
	}{
		{
			name: "INSERT",
			// The mysqlbinlog -vvv --base64-output=decode-rows output for the following event:
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=1 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='alice@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"salary": 100}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=1 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='alice@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_INSERT(@3, '$.role', 'manager') /* JSON meta=4 nullable=1 is_null=0 */
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=2 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='bob@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"salary": 99}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=2 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='bob@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_INSERT(@3, '$.role', 'manager') /* JSON meta=4 nullable=1 is_null=0 */
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=3 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='charlie@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"salary": 99}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=3 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='charlie@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_INSERT(@3, '$.role', 'manager') /* JSON meta=4 nullable=1 is_null=0 */
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=4 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='dan@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"salary": 99}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=4 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='dan@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_INSERT(@3, '$.role', 'manager') /* JSON meta=4 nullable=1 is_null=0 */
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=5 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='eve@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"salary": 100}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=5 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='eve@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_INSERT(@3, '$.role', 'manager') /* JSON meta=4 nullable=1 is_null=0 */
			rawEvent: []byte{
				196, 19, 87, 103, 39, 47, 142, 143, 12, 6, 2, 0, 0, 229, 104, 0, 0, 0, 0, 176, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 255, 255, 0, 1, 0, 0, 0, 0, 0,
				0, 0, 16, 97, 108, 105, 99, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 18, 0, 0, 0, 0, 1, 0, 17, 0, 11, 0, 6, 0, 5, 100, 0, 115,
				97, 108, 97, 114, 121, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 16, 97, 108, 105, 99, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 18, 0,
				0, 0, 1, 6, 36, 46, 114, 111, 108, 101, 9, 12, 7, 109, 97, 110, 97, 103, 101, 114, 0, 2, 0, 0, 0, 0, 0, 0, 0, 14, 98, 111, 98, 64, 100, 111,
				109, 97, 105, 110, 46, 99, 111, 109, 18, 0, 0, 0, 0, 1, 0, 17, 0, 11, 0, 6, 0, 5, 99, 0, 115, 97, 108, 97, 114, 121, 1, 1, 0, 2, 0, 0, 0, 0,
				0, 0, 0, 14, 98, 111, 98, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 18, 0, 0, 0, 1, 6, 36, 46, 114, 111, 108, 101, 9, 12, 7, 109, 97,
				110, 97, 103, 101, 114, 0, 3, 0, 0, 0, 0, 0, 0, 0, 18, 99, 104, 97, 114, 108, 105, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 18,
				0, 0, 0, 0, 1, 0, 17, 0, 11, 0, 6, 0, 5, 99, 0, 115, 97, 108, 97, 114, 121, 1, 1, 0, 3, 0, 0, 0, 0, 0, 0, 0, 18, 99, 104, 97, 114, 108, 105,
				101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 18, 0, 0, 0, 1, 6, 36, 46, 114, 111, 108, 101, 9, 12, 7, 109, 97, 110, 97, 103, 101,
				114, 0, 4, 0, 0, 0, 0, 0, 0, 0, 14, 100, 97, 110, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 18, 0, 0, 0, 0, 1, 0, 17, 0, 11, 0, 6, 0,
				5, 99, 0, 115, 97, 108, 97, 114, 121, 1, 1, 0, 4, 0, 0, 0, 0, 0, 0, 0, 14, 100, 97, 110, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 18,
				0, 0, 0, 1, 6, 36, 46, 114, 111, 108, 101, 9, 12, 7, 109, 97, 110, 97, 103, 101, 114, 0, 5, 0, 0, 0, 0, 0, 0, 0, 14, 101, 118, 101, 64, 100,
				111, 109, 97, 105, 110, 46, 99, 111, 109, 18, 0, 0, 0, 0, 1, 0, 17, 0, 11, 0, 6, 0, 5, 100, 0, 115, 97, 108, 97, 114, 121, 1, 1, 0, 5, 0, 0, 0,
				0, 0, 0, 0, 14, 101, 118, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 18, 0, 0, 0, 1, 6, 36, 46, 114, 111, 108, 101, 9, 12, 7, 109,
				97, 110, 97, 103, 101, 114,
			},
			numRows: 5,
			want:    "JSON_INSERT(%s, _utf8mb4'$.role', CAST(JSON_QUOTE(_utf8mb4'manager') as JSON))",
		},
		{
			// The mysqlbinlog -vvv --base64-output=decode-rows output for the following event:
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=1 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='alice@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"role": "manager", "salary": 100}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=1 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='alice@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_REPLACE(@3, '$.role', 'IC') /* JSON meta=4 nullable=1 is_null=0 */
			rawEvent: []byte{
				155, 21, 87, 103, 39, 47, 142, 143, 12, 148, 0, 0, 0, 135, 106, 0, 0, 0, 0, 176, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 255, 255, 0, 1, 0, 0, 0, 0, 0, 0,
				0, 16, 97, 108, 105, 99, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 37, 0, 0, 0, 0, 2, 0, 36, 0, 18, 0, 4, 0, 22, 0, 6, 0, 12, 28,
				0, 5, 100, 0, 114, 111, 108, 101, 115, 97, 108, 97, 114, 121, 7, 109, 97, 110, 97, 103, 101, 114, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 16, 97, 108,
				105, 99, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 13, 0, 0, 0, 0, 6, 36, 46, 114, 111, 108, 101, 4, 12, 2, 73, 67,
			},
			name:    "REPLACE",
			numRows: 1,
			want:    "JSON_REPLACE(%s, _utf8mb4'$.role', CAST(JSON_QUOTE(_utf8mb4'IC') as JSON))",
		},
		{
			name: "REMOVE",
			// The mysqlbinlog -vvv --base64-output=decode-rows output for the following event:
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=2 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='bob@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"role": "manager", "salary": 99}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=2 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='bob@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_REMOVE(@3, '$.salary') /* JSON meta=4 nullable=1 is_null=0 */
			numRows: 1,
			rawEvent: []byte{
				176, 22, 87, 103, 39, 47, 142, 143, 12, 141, 0, 0, 0, 34, 108, 0, 0, 0, 0, 176, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 255, 255, 0, 2, 0, 0, 0, 0, 0, 0, 0,
				14, 98, 111, 98, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 37, 0, 0, 0, 0, 2, 0, 36, 0, 18, 0, 4, 0, 22, 0, 6, 0, 12, 28, 0, 5, 99, 0, 114,
				111, 108, 101, 115, 97, 108, 97, 114, 121, 7, 109, 97, 110, 97, 103, 101, 114, 1, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 14, 98, 111, 98, 64, 100, 111, 109,
				97, 105, 110, 46, 99, 111, 109, 10, 0, 0, 0, 2, 8, 36, 46, 115, 97, 108, 97, 114, 121,
			},
			want: "JSON_REMOVE(%s, _utf8mb4'$.salary')",
		},
		{
			name: "REMOVE and REPLACE",
			// The mysqlbinlog -vvv --base64-output=decode-rows output for the following event:
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=1 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='alice@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "friday", "role": "manager", "color": "red", "salary": 100, "favorite_color": "black"}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=1 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='alice@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_REMOVE(
			// ###      JSON_REPLACE(@3, '$.day', 'monday'),
			// ###      '$.favorite_color') /* JSON meta=4 nullable=1 is_null=0 */
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=2 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='bob@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "friday", "role": "manager", "color": "red", "salary": 99, "favorite_color": "black"}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=2 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='bob@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_REMOVE(
			// ###      JSON_REPLACE(@3, '$.day', 'monday'),
			// ###      '$.favorite_color') /* JSON meta=4 nullable=1 is_null=0 */
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=3 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='charlie@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "friday", "role": "manager", "color": "red", "salary": 99, "favorite_color": "black"}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=3 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='charlie@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_REMOVE(
			// ###      JSON_REPLACE(@3, '$.day', 'monday'),
			// ###      '$.favorite_color') /* JSON meta=4 nullable=1 is_null=0 */
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=4 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='dan@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "friday", "role": "manager", "color": "red", "salary": 99, "favorite_color": "black"}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=4 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='dan@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_REMOVE(
			// ###      JSON_REPLACE(@3, '$.day', 'monday'),
			// ###      '$.favorite_color') /* JSON meta=4 nullable=1 is_null=0 */
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=5 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='eve@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "friday", "role": "manager", "color": "red", "salary": 100, "favorite_color": "black"}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=5 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='eve@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_REMOVE(
			// ###      JSON_REPLACE(@3, '$.day', 'monday'),
			// ###      '$.favorite_color') /* JSON meta=4 nullable=1 is_null=0 */
			rawEvent: []byte{
				227, 240, 86, 103, 39, 74, 58, 208, 33, 225, 3, 0, 0, 173, 122, 0, 0, 0, 0, 176, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 255, 255, 0, 1, 0, 0, 0, 0,
				0, 0, 0, 16, 97, 108, 105, 99, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 97, 0, 0, 0, 0, 5, 0, 96, 0, 39, 0, 3, 0, 42, 0, 4,
				0, 46, 0, 5, 0, 51, 0, 6, 0, 57, 0, 14, 0, 12, 71, 0, 12, 78, 0, 12, 86, 0, 5, 100, 0, 12, 90, 0, 100, 97, 121, 114, 111, 108, 101, 99, 111,
				108, 111, 114, 115, 97, 108, 97, 114, 121, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114, 6, 102, 114, 105, 100, 97, 121,
				7, 109, 97, 110, 97, 103, 101, 114, 3, 114, 101, 100, 5, 98, 108, 97, 99, 107, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 16, 97, 108, 105, 99, 101,
				64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 34, 0, 0, 0, 0, 5, 36, 46, 100, 97, 121, 8, 12, 6, 109, 111, 110, 100, 97, 121, 2, 16, 36,
				46, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114, 0, 2, 0, 0, 0, 0, 0, 0, 0, 14, 98, 111, 98, 64, 100, 111, 109, 97,
				105, 110, 46, 99, 111, 109, 97, 0, 0, 0, 0, 5, 0, 96, 0, 39, 0, 3, 0, 42, 0, 4, 0, 46, 0, 5, 0, 51, 0, 6, 0, 57, 0, 14, 0, 12, 71, 0, 12, 78,
				0, 12, 86, 0, 5, 99, 0, 12, 90, 0, 100, 97, 121, 114, 111, 108, 101, 99, 111, 108, 111, 114, 115, 97, 108, 97, 114, 121, 102, 97, 118, 111,
				114, 105, 116, 101, 95, 99, 111, 108, 111, 114, 6, 102, 114, 105, 100, 97, 121, 7, 109, 97, 110, 97, 103, 101, 114, 3, 114, 101, 100, 5, 98,
				108, 97, 99, 107, 1, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 14, 98, 111, 98, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 34, 0, 0, 0, 0, 5, 36,
				46, 100, 97, 121, 8, 12, 6, 109, 111, 110, 100, 97, 121, 2, 16, 36, 46, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114, 0,
				3, 0, 0, 0, 0, 0, 0, 0, 18, 99, 104, 97, 114, 108, 105, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 97, 0, 0, 0, 0, 5, 0, 96, 0,
				39, 0, 3, 0, 42, 0, 4, 0, 46, 0, 5, 0, 51, 0, 6, 0, 57, 0, 14, 0, 12, 71, 0, 12, 78, 0, 12, 86, 0, 5, 99, 0, 12, 90, 0, 100, 97, 121, 114,
				111, 108, 101, 99, 111, 108, 111, 114, 115, 97, 108, 97, 114, 121, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114, 6, 102,
				114, 105, 100, 97, 121, 7, 109, 97, 110, 97, 103, 101, 114, 3, 114, 101, 100, 5, 98, 108, 97, 99, 107, 1, 1, 0, 3, 0, 0, 0, 0, 0, 0, 0, 18,
				99, 104, 97, 114, 108, 105, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 34, 0, 0, 0, 0, 5, 36, 46, 100, 97, 121, 8, 12, 6, 109,
				111, 110, 100, 97, 121, 2, 16, 36, 46, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114, 0, 4, 0, 0, 0, 0, 0, 0, 0, 14, 100,
				97, 110, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 97, 0, 0, 0, 0, 5, 0, 96, 0, 39, 0, 3, 0, 42, 0, 4, 0, 46, 0, 5, 0, 51, 0, 6, 0,
				57, 0, 14, 0, 12, 71, 0, 12, 78, 0, 12, 86, 0, 5, 99, 0, 12, 90, 0, 100, 97, 121, 114, 111, 108, 101, 99, 111, 108, 111, 114, 115, 97, 108,
				97, 114, 121, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114, 6, 102, 114, 105, 100, 97, 121, 7, 109, 97, 110, 97, 103,
				101, 114, 3, 114, 101, 100, 5, 98, 108, 97, 99, 107, 1, 1, 0, 4, 0, 0, 0, 0, 0, 0, 0, 14, 100, 97, 110, 64, 100, 111, 109, 97, 105, 110, 46,
				99, 111, 109, 34, 0, 0, 0, 0, 5, 36, 46, 100, 97, 121, 8, 12, 6, 109, 111, 110, 100, 97, 121, 2, 16, 36, 46, 102, 97, 118, 111, 114, 105, 116,
				101, 95, 99, 111, 108, 111, 114, 0, 5, 0, 0, 0, 0, 0, 0, 0, 14, 101, 118, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 97, 0, 0, 0,
				0, 5, 0, 96, 0, 39, 0, 3, 0, 42, 0, 4, 0, 46, 0, 5, 0, 51, 0, 6, 0, 57, 0, 14, 0, 12, 71, 0, 12, 78, 0, 12, 86, 0, 5, 100, 0, 12, 90, 0, 100,
				97, 121, 114, 111, 108, 101, 99, 111, 108, 111, 114, 115, 97, 108, 97, 114, 121, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111,
				114, 6, 102, 114, 105, 100, 97, 121, 7, 109, 97, 110, 97, 103, 101, 114, 3, 114, 101, 100, 5, 98, 108, 97, 99, 107, 1, 1, 0, 5, 0, 0, 0, 0, 0,
				0, 0, 14, 101, 118, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 34, 0, 0, 0, 0, 5, 36, 46, 100, 97, 121, 8, 12, 6, 109, 111, 110,
				100, 97, 121, 2, 16, 36, 46, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114,
			},
			numRows: 5,
			want:    "JSON_REMOVE(JSON_REPLACE(%s, _utf8mb4'$.day', CAST(JSON_QUOTE(_utf8mb4'monday') as JSON)), _utf8mb4'$.favorite_color')",
		},
		{
			name: "INSERT and REMOVE and REPLACE",
			// The mysqlbinlog -vvv --base64-output=decode-rows output for the following event:
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=3 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='charlie@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "monday", "role": "manager", "salary": 99, "favorite_color": "red"}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=3 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='charlie@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_INSERT(
			// ###      JSON_REMOVE(
			// ###      JSON_REPLACE(@3, '$.day', 'tuesday'),
			// ###      '$.favorite_color'),
			// ###      '$.hobby', 'skiing') /* JSON meta=4 nullable=1 is_null=0 */
			rawEvent: []byte{
				48, 25, 87, 103, 39, 47, 142, 143, 12, 234, 0, 0, 0, 0, 117, 0, 0, 0, 0, 176, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 255, 255, 0, 3, 0, 0, 0, 0, 0, 0, 0,
				18, 99, 104, 97, 114, 108, 105, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 79, 0, 0, 0, 0, 4, 0, 78, 0, 32, 0, 3, 0, 35, 0, 4, 0,
				39, 0, 6, 0, 45, 0, 14, 0, 12, 59, 0, 12, 66, 0, 5, 99, 0, 12, 74, 0, 100, 97, 121, 114, 111, 108, 101, 115, 97, 108, 97, 114, 121, 102, 97,
				118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114, 6, 109, 111, 110, 100, 97, 121, 7, 109, 97, 110, 97, 103, 101, 114, 3, 114, 101, 100,
				1, 1, 0, 3, 0, 0, 0, 0, 0, 0, 0, 18, 99, 104, 97, 114, 108, 105, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 53, 0, 0, 0, 0, 5, 36,
				46, 100, 97, 121, 9, 12, 7, 116, 117, 101, 115, 100, 97, 121, 2, 16, 36, 46, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114,
				1, 7, 36, 46, 104, 111, 98, 98, 121, 8, 12, 6, 115, 107, 105, 105, 110, 103,
			},
			numRows: 1,
			want:    "JSON_INSERT(JSON_REMOVE(JSON_REPLACE(%s, _utf8mb4'$.day', CAST(JSON_QUOTE(_utf8mb4'tuesday') as JSON)), _utf8mb4'$.favorite_color'), _utf8mb4'$.hobby', CAST(JSON_QUOTE(_utf8mb4'skiing') as JSON))",
		},
		{
			name: "REPLACE with null",
			// The mysqlbinlog -vvv --base64-output=decode-rows output for the following event:
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=4 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='dan@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"role": "manager", "salary": 99}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=4 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='dan@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_REPLACE(@3, '$.salary', null) /* JSON meta=4 nullable=1 is_null=0 *
			rawEvent: []byte{
				148, 26, 87, 103, 39, 47, 142, 143, 12, 144, 0, 0, 0, 158, 118, 0, 0, 0, 0, 176, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 255, 255, 0, 4, 0, 0, 0, 0, 0, 0, 0,
				14, 100, 97, 110, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 37, 0, 0, 0, 0, 2, 0, 36, 0, 18, 0, 4, 0, 22, 0, 6, 0, 12, 28, 0, 5, 99, 0,
				114, 111, 108, 101, 115, 97, 108, 97, 114, 121, 7, 109, 97, 110, 97, 103, 101, 114, 1, 1, 0, 4, 0, 0, 0, 0, 0, 0, 0, 14, 100, 97, 110, 64, 100, 111,
				109, 97, 105, 110, 46, 99, 111, 109, 13, 0, 0, 0, 0, 8, 36, 46, 115, 97, 108, 97, 114, 121, 2, 4, 0,
			},
			numRows: 1,
			want:    "JSON_REPLACE(%s, _utf8mb4'$.salary', CAST(_utf8mb4'null' as JSON))",
		},
		{
			name: "REPLACE 2 paths",
			// The mysqlbinlog -vvv --base64-output=decode-rows output for the following event:
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=4 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='dan@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"role": "manager", "salary": null}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=4 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='dan@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_REPLACE(@3, '$.salary', 110,
			// ###      '$.role', 'IC') /* JSON meta=4 nullable=1 is_null=0 */
			rawEvent: []byte{
				32, 32, 87, 103, 39, 26, 45, 78, 117, 158, 0, 0, 0, 145, 106, 0, 0, 0, 0, 176, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 255, 255, 0, 4, 0, 0, 0, 0, 0, 0, 0, 14,
				100, 97, 110, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 37, 0, 0, 0, 0, 2, 0, 36, 0, 18, 0, 4, 0, 22, 0, 6, 0, 12, 28, 0, 5, 99, 0, 114, 111,
				108, 101, 115, 97, 108, 97, 114, 121, 7, 109, 97, 110, 97, 103, 101, 114, 1, 1, 0, 4, 0, 0, 0, 0, 0, 0, 0, 14, 100, 97, 110, 64, 100, 111, 109, 97,
				105, 110, 46, 99, 111, 109, 27, 0, 0, 0, 0, 8, 36, 46, 115, 97, 108, 97, 114, 121, 3, 5, 110, 0, 0, 6, 36, 46, 114, 111, 108, 101, 4, 12, 2, 73, 67,
			},
			numRows: 1,
			want:    "JSON_REPLACE(JSON_REPLACE(%s, _utf8mb4'$.salary', CAST(110 as JSON)), _utf8mb4'$.role', CAST(JSON_QUOTE(_utf8mb4'IC') as JSON))",
		},
		{
			name: "JSON null",
			// The mysqlbinlog -vvv --base64-output=decode-rows output for the following event:
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=5 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='neweve@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "friday", "role": "manager", "color": "red", "salary": 100, "favorite_color": "black"}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=5 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='neweve@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='null' /* JSON meta=4 nullable=1 is_null=0 */
			rawEvent: []byte{
				109, 200, 88, 103, 39, 57, 91, 186, 0, 194, 0, 0, 0, 0, 0, 0, 0, 0, 0, 178, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 7, 7, 0, 5, 0, 0, 0, 0, 0, 0, 0, 17, 110,
				101, 119, 101, 118, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 97, 0, 0, 0, 0, 5, 0, 96, 0, 39, 0, 3, 0, 42, 0, 4, 0, 46, 0, 5, 0, 51,
				0, 6, 0, 57, 0, 14, 0, 12, 71, 0, 12, 78, 0, 12, 86, 0, 5, 100, 0, 12, 90, 0, 100, 97, 121, 114, 111, 108, 101, 99, 111, 108, 111, 114, 115, 97,
				108, 97, 114, 121, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114, 6, 102, 114, 105, 100, 97, 121, 7, 109, 97, 110, 97, 103, 101,
				114, 3, 114, 101, 100, 5, 98, 108, 97, 99, 107, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 17, 110, 101, 119, 101, 118, 101, 64, 100, 111, 109, 97, 105, 110, 46,
				99, 111, 109, 2, 0, 0, 0, 4, 0,
			},
			numRows: 1,
			want:    "null",
		},
		{
			name: "null literal string",
			// The mysqlbinlog -vvv --base64-output=decode-rows output for the following event:
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=10 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='mlord@planetscale.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=NULL /* JSON meta=4 nullable=1 is_null=1 */
			// ### SET
			// ###   @1=10 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='mlord@planetscale.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='null' /* JSON meta=4 nullable=1 is_null=0 */
			rawEvent: []byte{
				178, 168, 89, 103, 39, 37, 191, 137, 18, 105, 0, 0, 0, 0, 0, 0, 0, 0, 0, 153, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 7, 7, 4, 10, 0, 0, 0, 0, 0, 0, 0, 21,
				109, 108, 111, 114, 100, 64, 112, 108, 97, 110, 101, 116, 115, 99, 97, 108, 101, 46, 99, 111, 109, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 21, 109, 108, 111,
				114, 100, 64, 112, 108, 97, 110, 101, 116, 115, 99, 97, 108, 101, 46, 99, 111, 109, 6, 0, 0, 0, 12, 4, 110, 117, 108, 108,
			},
			numRows: 1,
			want:    "\"null\"",
		},
		{
			name: "JSON object",
			// The mysqlbinlog -vvv --base64-output=decode-rows output for the following event:
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=1 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='newalice@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "wednesday", "role": "manager", "color": "red", "salary": 100}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=1 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='newalice@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=JSON_INSERT(@3, '$.misc', '{"address":"1012 S Park", "town":"Hastings", "state":"MI"}') /* JSON meta=4 nullable=1 is_null=0 */
			rawEvent: []byte{
				208, 160, 89, 103, 39, 202, 59, 214, 68, 242, 0, 0, 0, 0, 0, 0, 0, 0, 0, 153, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 7, 7, 0, 1, 0, 0, 0, 0, 0, 0, 0, 19, 110,
				101, 119, 97, 108, 105, 99, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 73, 0, 0, 0, 0, 4, 0, 72, 0, 32, 0, 3, 0, 35, 0, 4, 0, 39, 0, 5,
				0, 44, 0, 6, 0, 12, 50, 0, 12, 60, 0, 12, 68, 0, 5, 100, 0, 100, 97, 121, 114, 111, 108, 101, 99, 111, 108, 111, 114, 115, 97, 108, 97, 114, 121, 9,
				119, 101, 100, 110, 101, 115, 100, 97, 121, 7, 109, 97, 110, 97, 103, 101, 114, 3, 114, 101, 100, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 19, 110, 101, 119,
				97, 108, 105, 99, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 69, 0, 0, 0, 1, 6, 36, 46, 109, 105, 115, 99, 60, 12, 58, 123, 34, 97, 100,
				100, 114, 101, 115, 115, 34, 58, 34, 49, 48, 49, 50, 32, 83, 32, 80, 97, 114, 107, 34, 44, 32, 34, 116, 111, 119, 110, 34, 58, 34, 72, 97, 115, 116,
				105, 110, 103, 115, 34, 44, 32, 34, 115, 116, 97, 116, 101, 34, 58, 34, 77, 73, 34, 125,
			},
			numRows: 1,
			want:    "JSON_INSERT(%s, _utf8mb4'$.misc', CAST(JSON_QUOTE(_utf8mb4'{\"address\":\"1012 S Park\", \"town\":\"Hastings\", \"state\":\"MI\"}') as JSON))",
		},
		{
			name: "JSON field not updated",
			// The mysqlbinlog -vvv --base64-output=decode-rows output for the following event:
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=1 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='newalice@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "friday", "role": "manager", "color": "red", "salary": 100, "favorite_color": "black"}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=101 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='newalice@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=@3 /* JSON meta=4 nullable=1 is_null=0 */
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=2 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='newbob@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "friday", "role": "manager", "color": "red", "salary": 99, "favorite_color": "black"}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=102 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='newbob@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=@3 /* JSON meta=4 nullable=1 is_null=0 */
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=3 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='newcharlie@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "monday", "role": "manager", "color": "red", "hobby": "skiing", "salary": 99}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=103 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='newcharlie@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=@3 /* JSON meta=4 nullable=1 is_null=0 */
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=4 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='newdan@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "friday", "role": "manager", "color": "red", "salary": 99, "favorite_color": "black"}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=104 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='newdan@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=@3 /* JSON meta=4 nullable=1 is_null=0 */
			// ### UPDATE `vt_commerce`.`customer`
			// ### WHERE
			// ###   @1=5 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='neweve@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3='{"day": "friday", "role": "manager", "color": "red", "salary": 100, "favorite_color": "black"}' /* JSON meta=4 nullable=1 is_null=0 */
			// ### SET
			// ###   @1=105 /* LONGINT meta=0 nullable=0 is_null=0 */
			// ###   @2='neweve@domain.com' /* VARSTRING(128) meta=128 nullable=1 is_null=0 */
			// ###   @3=@3 /* JSON meta=4 nullable=1 is_null=0 */
			rawEvent: []byte{
				194, 74, 100, 103, 39, 46, 144, 133, 54, 77, 3, 0, 0, 77, 128, 0, 0, 0, 0, 153, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 255, 255, 0, 1, 0, 0, 0, 0, 0, 0, 0,
				19, 110, 101, 119, 97, 108, 105, 99, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 97, 0, 0, 0, 0, 5, 0, 96, 0, 39, 0, 3, 0, 42, 0, 4, 0,
				46, 0, 5, 0, 51, 0, 6, 0, 57, 0, 14, 0, 12, 71, 0, 12, 78, 0, 12, 86, 0, 5, 100, 0, 12, 90, 0, 100, 97, 121, 114, 111, 108, 101, 99, 111, 108, 111,
				114, 115, 97, 108, 97, 114, 121, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114, 6, 102, 114, 105, 100, 97, 121, 7, 109, 97,
				110, 97, 103, 101, 114, 3, 114, 101, 100, 5, 98, 108, 97, 99, 107, 1, 1, 0, 101, 0, 0, 0, 0, 0, 0, 0, 19, 110, 101, 119, 97, 108, 105, 99, 101, 64,
				100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 17, 110, 101, 119, 98, 111, 98, 64, 100, 111, 109, 97, 105,
				110, 46, 99, 111, 109, 97, 0, 0, 0, 0, 5, 0, 96, 0, 39, 0, 3, 0, 42, 0, 4, 0, 46, 0, 5, 0, 51, 0, 6, 0, 57, 0, 14, 0, 12, 71, 0, 12, 78, 0, 12, 86,
				0, 5, 99, 0, 12, 90, 0, 100, 97, 121, 114, 111, 108, 101, 99, 111, 108, 111, 114, 115, 97, 108, 97, 114, 121, 102, 97, 118, 111, 114, 105, 116, 101,
				95, 99, 111, 108, 111, 114, 6, 102, 114, 105, 100, 97, 121, 7, 109, 97, 110, 97, 103, 101, 114, 3, 114, 101, 100, 5, 98, 108, 97, 99, 107, 1, 1, 0,
				102, 0, 0, 0, 0, 0, 0, 0, 17, 110, 101, 119, 98, 111, 98, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
				21, 110, 101, 119, 99, 104, 97, 114, 108, 105, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 89, 0, 0, 0, 0, 5, 0, 88, 0, 39, 0, 3, 0, 42,
				0, 4, 0, 46, 0, 5, 0, 51, 0, 5, 0, 56, 0, 6, 0, 12, 62, 0, 12, 69, 0, 12, 77, 0, 12, 81, 0, 5, 99, 0, 100, 97, 121, 114, 111, 108, 101, 99, 111, 108,
				111, 114, 104, 111, 98, 98, 121, 115, 97, 108, 97, 114, 121, 6, 109, 111, 110, 100, 97, 121, 7, 109, 97, 110, 97, 103, 101, 114, 3, 114, 101, 100, 6,
				115, 107, 105, 105, 110, 103, 1, 1, 0, 103, 0, 0, 0, 0, 0, 0, 0, 21, 110, 101, 119, 99, 104, 97, 114, 108, 105, 101, 64, 100, 111, 109, 97, 105, 110,
				46, 99, 111, 109, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 17, 110, 101, 119, 100, 97, 110, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 97, 0,
				0, 0, 0, 5, 0, 96, 0, 39, 0, 3, 0, 42, 0, 4, 0, 46, 0, 5, 0, 51, 0, 6, 0, 57, 0, 14, 0, 12, 71, 0, 12, 78, 0, 12, 86, 0, 5, 99, 0, 12, 90, 0, 100,
				97, 121, 114, 111, 108, 101, 99, 111, 108, 111, 114, 115, 97, 108, 97, 114, 121, 102, 97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114,
				6, 102, 114, 105, 100, 97, 121, 7, 109, 97, 110, 97, 103, 101, 114, 3, 114, 101, 100, 5, 98, 108, 97, 99, 107, 1, 1, 0, 104, 0, 0, 0, 0, 0, 0, 0, 17,
				110, 101, 119, 100, 97, 110, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 17, 110, 101, 119, 101, 118,
				101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 97, 0, 0, 0, 0, 5, 0, 96, 0, 39, 0, 3, 0, 42, 0, 4, 0, 46, 0, 5, 0, 51, 0, 6, 0, 57, 0, 14,
				0, 12, 71, 0, 12, 78, 0, 12, 86, 0, 5, 100, 0, 12, 90, 0, 100, 97, 121, 114, 111, 108, 101, 99, 111, 108, 111, 114, 115, 97, 108, 97, 114, 121, 102,
				97, 118, 111, 114, 105, 116, 101, 95, 99, 111, 108, 111, 114, 6, 102, 114, 105, 100, 97, 121, 7, 109, 97, 110, 97, 103, 101, 114, 3, 114, 101, 100,
				5, 98, 108, 97, 99, 107, 1, 1, 0, 105, 0, 0, 0, 0, 0, 0, 0, 17, 110, 101, 119, 101, 118, 101, 64, 100, 111, 109, 97, 105, 110, 46, 99, 111, 109, 0,
				0, 0, 0,
			},
			numRows: 5,
			want:    "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mysql56PartialUpdateRowEvent := NewMysql56BinlogEvent(tc.rawEvent)
			require.True(t, mysql56PartialUpdateRowEvent.IsPartialUpdateRows())

			ev, err := mysql56PartialUpdateRowEvent.Rows(format, tm)
			require.NoError(t, err)

			assert.Equal(t, tc.numRows, len(ev.Rows))
			require.NoError(t, err)

			for i := range ev.Rows {
				vals, err := ev.StringValuesForTests(tm, i)
				require.NoError(t, err)
				// The third column is the JSON column.
				require.Equal(t, tc.want, vals[2])
				t.Logf("Rows: %v", vals)
			}
		})
	}
}
