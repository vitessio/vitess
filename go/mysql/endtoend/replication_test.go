/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package endtoend

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// connectForReplication is a helper method to connect for replication
// from the current binlog position.
func connectForReplication(t *testing.T, rbr bool) (*mysql.Conn, mysql.BinlogFormat) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}

	// We need to know if this is MariaDB, to set the right flag.
	if conn.IsMariaDB() {
		// This flag is required to get GTIDs from MariaDB.
		t.Log("MariaDB: sensing SET @mariadb_slave_capability=4")
		if _, err := conn.ExecuteFetch("SET @mariadb_slave_capability=4", 0, false); err != nil {
			t.Fatalf("failed to set @mariadb_slave_capability=4: %v", err)
		}
	}

	// Switch server to RBR if needed.
	if rbr {
		if _, err := conn.ExecuteFetch("SET GLOBAL binlog_format='ROW'", 0, false); err != nil {
			t.Fatalf("SET GLOBAL binlog_format='ROW' failed: %v", err)
		}
	}

	// First we get the current binlog position.
	result, err := conn.ExecuteFetch("SHOW MASTER STATUS", 1, true)
	if err != nil {
		t.Fatalf("SHOW MASTER STATUS failed: %v", err)
	}
	if len(result.Fields) < 2 || result.Fields[0].Name != "File" || result.Fields[1].Name != "Position" ||
		len(result.Rows) != 1 {
		t.Fatalf("SHOW MASTER STATUS returned unexpected result: %v", result)
	}
	file := result.Rows[0][0].ToString()
	position, err := sqltypes.ToUint64(result.Rows[0][1])
	if err != nil {
		t.Fatalf("SHOW MASTER STATUS returned invalid position: %v", result.Rows[0][1])
	}

	// Tell the server that we understand the format of events
	// that will be used if binlog_checksum is enabled on the server.
	if _, err := conn.ExecuteFetch("SET @master_binlog_checksum=@@global.binlog_checksum", 0, false); err != nil {
		t.Fatalf("failed to set @master_binlog_checksum=@@global.binlog_checksum: %v", err)
	}

	// Write ComBinlogDump packet with to start streaming events from here.
	if err := conn.WriteComBinlogDump(1, file, uint32(position), 0); err != nil {
		t.Fatalf("WriteComBinlogDump failed: %v", err)
	}

	// Wait for the FORMAT_DESCRIPTION_EVENT
	var f mysql.BinlogFormat
	for {
		data, err := conn.ReadPacket()
		if err != nil {
			t.Fatalf("ReadPacket failed: %v", err)
		}

		// Make sure it's a replication packet.
		switch data[0] {
		case mysql.OKPacket:
			// What we expect, handled below.
		case mysql.ErrPacket:
			err := mysql.ParseErrorPacket(data)
			t.Fatalf("ReadPacket returned an error packet: %v", err)
		default:
			// Very unexpected.
			t.Fatalf("ReadPacket returned a weird packet: %v", data)
		}

		// See what we got.
		be := conn.MakeBinlogEvent(data[1:])
		if !be.IsValid() {
			t.Fatalf("NewMysql56BinlogEvent has an invalid packet: %v", be)
		}

		// Skip rotate packets. These are normal as first packets.
		if be.IsRotate() {
			t.Logf("Got a rotate packet: %v", data[20:])
			continue
		}

		// And we want a FORMAT_DESCRIPTION_EVENT.
		// Print a few things about the event for sanity checks.
		if !be.IsFormatDescription() {
			t.Fatalf("Unexpected packet: %v", be)
		}
		f, err = be.Format()
		if err != nil {
			t.Fatalf("Format() returned error: %v", err)
		}
		t.Logf("Got a FORMAT_DESCRIPTION_EVENT packet: %v\nWith format: %v", be, f)
		break
	}

	return conn, f
}

// TestReplicationConnectionClosing connects as a replication client,
// gets the first packet, then waits a few milliseconds and closes the
// connection. We should get the right error.
func TestReplicationConnectionClosing(t *testing.T) {
	conn, _ := connectForReplication(t, false /* rbr */)
	defer conn.Close()

	// One go routine is waiting on events.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			data, err := conn.ReadPacket()
			if err != nil {
				serr, ok := err.(*mysql.SQLError)
				if !ok {
					t.Fatalf("Got a non mysql.SQLError error: %v", err)
				}
				if serr.Num != mysql.CRServerLost {
					t.Fatalf("Got an unexpected mysql.SQLError error: %v", serr)
				}
				// we got the right error, all good.
				return
			}

			// Make sure it's a replication packet.
			switch data[0] {
			case mysql.OKPacket:
				// What we expect, keep going.
			case mysql.ErrPacket:
				err := mysql.ParseErrorPacket(data)
				t.Fatalf("ReadPacket returned an error packet: %v", err)
			default:
				// Very unexpected.
				t.Fatalf("ReadPacket returned a weird packet: %v", data)
			}
		}
	}()

	// Connect and create a table.
	ctx := context.Background()
	dConn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}
	defer dConn.Close()
	createTable := "create table replicationError(id int, name varchar(128), primary key(id))"
	if _, err := dConn.ExecuteFetch(createTable, 0, false); err != nil {
		t.Fatal(err)
	}
	result, err := dConn.ExecuteFetch("insert into replicationError(id, name) values(10, 'nice name')", 0, false)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if result.RowsAffected != 1 || len(result.Rows) != 0 {
		t.Errorf("unexpected result for insert: %v", result)
	}
	if _, err := dConn.ExecuteFetch("drop table replicationError", 0, false); err != nil {
		t.Fatalf("drop table failed: %v", err)
	}

	// wait for a few milliseconds.
	time.Sleep(10 * time.Millisecond)

	// Close the replication connection, hopefully while we are
	// reading packets from it.
	conn.Close()

	// And we wait for background routine to exit.
	wg.Wait()
}

func TestStatementReplicationWithRealDatabase(t *testing.T) {
	conn, f := connectForReplication(t, false /* rbr */)
	defer conn.Close()

	// Create a table, insert some data in it.
	ctx := context.Background()
	dConn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}
	defer dConn.Close()
	createTable := "create table replication(id int, name varchar(128), primary key(id))"
	if _, err := dConn.ExecuteFetch(createTable, 0, false); err != nil {
		t.Fatal(err)
	}
	insert := "insert into replication(id, name) values(10, 'nice name')"
	result, err := dConn.ExecuteFetch(insert, 0, false)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if result.RowsAffected != 1 || len(result.Rows) != 0 {
		t.Errorf("unexpected result for insert: %v", result)
	}

	// Get the new events from the binlogs.
	// Make sure we get two GTIDs, the table creation event and the insert
	// (with both a begin and a commit).
	gtidCount := 0
	gotCreateTable := false
	gotBegin := false
	gotInsert := false
	gotCommit := false
	for gtidCount < 2 || !gotCreateTable || !gotBegin || !gotInsert || !gotCommit {
		data, err := conn.ReadPacket()
		if err != nil {
			t.Fatalf("ReadPacket failed: %v", err)
		}

		// Make sure it's a replication packet.
		switch data[0] {
		case mysql.OKPacket:
			// What we expect, handled below.
		case mysql.ErrPacket:
			err := mysql.ParseErrorPacket(data)
			t.Fatalf("ReadPacket returned an error packet: %v", err)
		default:
			// Very unexpected.
			t.Fatalf("ReadPacket returned a weird packet: %v", data)
		}

		// See what we got, strip the checksum.
		be := conn.MakeBinlogEvent(data[1:])
		if !be.IsValid() {
			t.Fatalf("read an invalid packet: %v", be)
		}
		be, _, err = be.StripChecksum(f)
		if err != nil {
			t.Fatalf("StripChecksum failed: %v", err)
		}
		switch {
		case be.IsGTID():
			// We expect one of these at least.
			gtid, hasBegin, err := be.GTID(f)
			if err != nil {
				t.Fatalf("GTID event is broken: %v", err)
			}
			t.Logf("Got GTID event: %v %v", gtid, hasBegin)
			gtidCount++
			if hasBegin {
				gotBegin = true
			}
		case be.IsQuery():
			q, err := be.Query(f)
			if err != nil {
				t.Fatalf("Query event is broken: %v", err)
			}
			t.Logf("Got Query event: %v", q)
			switch strings.ToLower(q.SQL) {
			case createTable:
				gotCreateTable = true
			case insert:
				gotInsert = true
			case "begin":
				gotBegin = true
			case "commit":
				gotCommit = true
			}
		case be.IsXID():
			gotCommit = true
			t.Logf("Got XID event")
		default:
			t.Logf("Got unrelated event: %v", be)
		}
	}

	// Drop the table, we're done.
	if _, err := dConn.ExecuteFetch("drop table replication", 0, false); err != nil {
		t.Fatal(err)
	}

}

func TestRowReplicationWithRealDatabase(t *testing.T) {
	conn, f := connectForReplication(t, true /* rbr */)
	defer conn.Close()

	// Create a table, insert some data in it.
	ctx := context.Background()
	dConn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}
	defer dConn.Close()
	createTable := "create table replication(id int, name varchar(128), primary key(id))"
	if _, err := dConn.ExecuteFetch(createTable, 0, false); err != nil {
		t.Fatal(err)
	}
	result, err := dConn.ExecuteFetch("insert into replication(id, name) values(10, 'nice name')", 0, false)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if result.RowsAffected != 1 || len(result.Rows) != 0 {
		t.Errorf("unexpected result for insert: %v", result)
	}
	result, err = dConn.ExecuteFetch("update replication set name='nicer name' where id=10", 0, false)
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if result.RowsAffected != 1 || len(result.Rows) != 0 {
		t.Errorf("unexpected result for update: %v", result)
	}
	result, err = dConn.ExecuteFetch("delete from replication where id=10", 0, false)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if result.RowsAffected != 1 || len(result.Rows) != 0 {
		t.Errorf("unexpected result for delete: %v", result)
	}

	// Get the new events from the binlogs.
	// Make sure we get four GTIDs, the table creation event, the
	// table map event, and the insert/update/delete (with both a begin and a commit).
	gtidCount := 0
	beginCount := 0
	commitCount := 0
	gotCreateTable := false
	gotTableMapEvent := false
	gotInsert := false
	gotUpdate := false
	gotDelete := false

	var tableID uint64
	var tableMap *mysql.TableMap

	//	for i := 0; i < 6 && (gtidCount < 2 || !gotCreateTable || !gotTableMapEvent || !gotBegin || !gotInsert || !gotCommit); i++ {
	for gtidCount < 4 || !gotCreateTable || !gotTableMapEvent || !gotInsert || !gotUpdate || !gotDelete || beginCount != 3 || commitCount != 3 {
		data, err := conn.ReadPacket()
		if err != nil {
			t.Fatalf("ReadPacket failed: %v", err)
		}

		// Make sure it's a replication packet.
		switch data[0] {
		case mysql.OKPacket:
			// What we expect, handled below.
		case mysql.ErrPacket:
			err := mysql.ParseErrorPacket(data)
			t.Fatalf("ReadPacket returned an error packet: %v", err)
		default:
			// Very unexpected.
			t.Fatalf("ReadPacket returned a weird packet: %v", data)
		}

		// See what we got, strip the checksum.
		be := conn.MakeBinlogEvent(data[1:])
		if !be.IsValid() {
			t.Fatalf("read an invalid packet: %v", be)
		}
		be, _, err = be.StripChecksum(f)
		if err != nil {
			t.Fatalf("StripChecksum failed: %v", err)
		}
		switch {
		case be.IsGTID():
			// We expect one of these at least.
			gtid, hasBegin, err := be.GTID(f)
			if err != nil {
				t.Fatalf("GTID event is broken: %v", err)
			}
			t.Logf("Got GTID event: %v %v", gtid, hasBegin)
			gtidCount++
			if hasBegin {
				beginCount++
			}
		case be.IsQuery():
			q, err := be.Query(f)
			if err != nil {
				t.Fatalf("Query event is broken: %v", err)
			}
			t.Logf("Got Query event: %v", q)
			switch strings.ToLower(q.SQL) {
			case createTable:
				gotCreateTable = true
			case "begin":
				beginCount++
			case "commit":
				commitCount++
			}
		case be.IsXID():
			commitCount++
			t.Logf("Got XID event")
		case be.IsTableMap():
			tableID = be.TableID(f) // This would be 0x00ffffff for an event to clear all table map entries.
			var err error
			tableMap, err = be.TableMap(f)
			if err != nil {
				t.Fatalf("TableMap event is broken: %v", err)
			}
			t.Logf("Got Table Map event: %v %v", tableID, tableMap)
			if tableMap.Database != "vttest" ||
				tableMap.Name != "replication" ||
				len(tableMap.Types) != 2 ||
				tableMap.CanBeNull.Bit(0) ||
				!tableMap.CanBeNull.Bit(1) {
				t.Errorf("got wrong TableMap: %v", tableMap)
			}
			gotTableMapEvent = true
		case be.IsWriteRows():
			if got := be.TableID(f); got != tableID {
				t.Fatalf("WriteRows event got table ID %v but was expecting %v", got, tableID)
			}
			wr, err := be.Rows(f, tableMap)
			if err != nil {
				t.Fatalf("Rows event is broken: %v", err)
			}

			// Check it has 2 rows, and first value is '10', second value is 'nice name'.
			values, _ := wr.StringValuesForTests(tableMap, 0)
			t.Logf("Got WriteRows event data: %v %v", wr, values)
			if expected := []string{"10", "nice name"}; !reflect.DeepEqual(values, expected) {
				t.Fatalf("StringValues returned %v, expected %v", values, expected)
			}

			gotInsert = true
		case be.IsUpdateRows():
			if got := be.TableID(f); got != tableID {
				t.Fatalf("UpdateRows event got table ID %v but was expecting %v", got, tableID)
			}
			ur, err := be.Rows(f, tableMap)
			if err != nil {
				t.Fatalf("UpdateRows event is broken: %v", err)
			}

			// Check it has 2 identify rows, and first value is '10', second value is 'nice name'.
			values, _ := ur.StringIdentifiesForTests(tableMap, 0)
			t.Logf("Got UpdateRows event identify: %v %v", ur, values)
			if expected := []string{"10", "nice name"}; !reflect.DeepEqual(values, expected) {
				t.Fatalf("StringIdentifies returned %v, expected %v", values, expected)
			}

			// Check it has 2 values rows, and first value is '10', second value is 'nicer name'.
			values, _ = ur.StringValuesForTests(tableMap, 0)
			t.Logf("Got UpdateRows event data: %v %v", ur, values)
			if expected := []string{"10", "nicer name"}; !reflect.DeepEqual(values, expected) {
				t.Fatalf("StringValues returned %v, expected %v", values, expected)
			}

			gotUpdate = true
		case be.IsDeleteRows():
			if got := be.TableID(f); got != tableID {
				t.Fatalf("DeleteRows event got table ID %v but was expecting %v", got, tableID)
			}
			dr, err := be.Rows(f, tableMap)
			if err != nil {
				t.Fatalf("DeleteRows event is broken: %v", err)
			}

			// Check it has 2 rows, and first value is '10', second value is 'nicer name'.
			values, _ := dr.StringIdentifiesForTests(tableMap, 0)
			t.Logf("Got DeleteRows event identify: %v %v", dr, values)
			if expected := []string{"10", "nicer name"}; !reflect.DeepEqual(values, expected) {
				t.Fatalf("StringIdentifies returned %v, expected %v", values, expected)
			}

			gotDelete = true
		default:
			t.Logf("Got unrelated event: %v", be)
		}
	}

	// Drop the table, we're done.
	if _, err := dConn.ExecuteFetch("drop table replication", 0, false); err != nil {
		t.Fatal(err)
	}

}

// TestRowReplicationTypes creates a table wih all
// supported data types. Then we insert a row in it. then we re-build
// the SQL for the values, re-insert these. Then we select from the
// database and make sure both rows are identical.
func TestRowReplicationTypes(t *testing.T) {
	// testcases are ordered by the types numbers in constants.go.
	// Number are always unsigned, as we don't pass in sqltypes.Type.
	testcases := []struct {
		name        string
		createType  string
		createValue string
	}{{
		// TINYINT
		name:        "tinytiny",
		createType:  "TINYINT UNSIGNED",
		createValue: "145",
	}, {
		// SMALLINT
		name:        "smallish",
		createType:  "SMALLINT UNSIGNED",
		createValue: "40000",
	}, {
		// INT
		name:        "regular_int",
		createType:  "INT UNSIGNED",
		createValue: "4000000000",
	}, {
		// FLOAT
		name:        "floating",
		createType:  "FLOAT",
		createValue: "-3.14159E-22",
	}, {
		// DOUBLE
		name:        "doubling",
		createType:  "DOUBLE",
		createValue: "-3.14159265359E+12",
	}, {
		// TIMESTAMP (zero value)
		name:        "timestamp_zero",
		createType:  "TIMESTAMP",
		createValue: "'0000-00-00 00:00:00'",
	}, {
		// TIMESTAMP (day precision)
		name:        "timestamp_day",
		createType:  "TIMESTAMP",
		createValue: "'2012-11-10 00:00:00'",
	}, {
		// BIGINT
		name:        "big_int",
		createType:  "BIGINT UNSIGNED",
		createValue: "10000000000000000000",
	}, {
		// MEDIUMINT
		name:        "mediumish",
		createType:  "MEDIUMINT UNSIGNED",
		createValue: "10000000",
	}, {
		// DATE
		name:        "date_regular",
		createType:  "DATE",
		createValue: "'1920-10-24'",
	}, {
		// TIME
		name:        "time_regular",
		createType:  "TIME",
		createValue: "'120:44:58'",
	}, {
		// TIME
		name:        "time_neg",
		createType:  "TIME",
		createValue: "'-212:44:58'",
	}, {
		// DATETIME
		name:        "datetime0",
		createType:  "DATETIME",
		createValue: "'1020-08-23 12:44:58'",
	}, {
		// YEAR zero
		name:        "year0",
		createType:  "YEAR",
		createValue: "0",
	}, {
		// YEAR
		name:        "year_nonzero",
		createType:  "YEAR",
		createValue: "2052",
	}, {
		// VARCHAR 8 bits
		name:        "shortvc",
		createType:  "VARCHAR(30)",
		createValue: "'short varchar'",
	}, {
		// VARCHAR 16 bits
		name:        "longvc",
		createType:  "VARCHAR(1000)",
		createValue: "'long varchar'",
	}, {
		// BIT
		name:        "bit1",
		createType:  "BIT",
		createValue: "b'1'",
	}, {
		// BIT
		name:        "bit6",
		createType:  "BIT(6)",
		createValue: "b'100101'",
	}, {
		// BIT
		name:        "bit8",
		createType:  "BIT(8)",
		createValue: "b'10100101'",
	}, {
		// BIT
		name:        "bit14",
		createType:  "BIT(14)",
		createValue: "b'10100101000111'",
	}, {
		// BIT
		name:        "bit55",
		createType:  "BIT(55)",
		createValue: "b'1010010100110100101001101001010011010010100110100101001'",
	}, {
		// BIT
		name:        "bit64",
		createType:  "BIT(64)",
		createValue: "b'1111111111010010100110100101001101001010011010010100110100101001'",
	}, {
		// DECIMAL
		name:        "decimal2_1",
		createType:  "DECIMAL(2,1)",
		createValue: "1.2",
	}, {
		// DECIMAL neg
		name:        "decimal2_1_neg",
		createType:  "DECIMAL(2,1)",
		createValue: "-5.6",
	}, {
		// DECIMAL
		name:        "decimal4_2",
		createType:  "DECIMAL(4,2)",
		createValue: "61.52",
	}, {
		// DECIMAL neg
		name:        "decimal4_2_neg",
		createType:  "DECIMAL(4,2)",
		createValue: "-78.94",
	}, {
		// DECIMAL
		name:        "decimal6_3",
		createType:  "DECIMAL(6,3)",
		createValue: "611.542",
	}, {
		// DECIMAL neg
		name:        "decimal6_3_neg",
		createType:  "DECIMAL(6,3)",
		createValue: "-478.394",
	}, {
		// DECIMAL
		name:        "decimal8_4",
		createType:  "DECIMAL(8,4)",
		createValue: "6311.5742",
	}, {
		// DECIMAL neg
		name:        "decimal8_4_neg",
		createType:  "DECIMAL(8,4)",
		createValue: "-4778.3894",
	}, {
		// DECIMAL
		name:        "decimal10_5",
		createType:  "DECIMAL(10,5)",
		createValue: "63711.57342",
	}, {
		// DECIMAL neg
		name:        "decimal10_5_neg",
		createType:  "DECIMAL(10,5)",
		createValue: "-47378.38594",
	}, {
		// DECIMAL
		name:        "decimal12_6",
		createType:  "DECIMAL(12,6)",
		createValue: "637311.557342",
	}, {
		// DECIMAL neg
		name:        "decimal12_6_neg",
		createType:  "DECIMAL(12,6)",
		createValue: "-473788.385794",
	}, {
		// DECIMAL
		name:        "decimal14_7",
		createType:  "DECIMAL(14,7)",
		createValue: "6375311.5574342",
	}, {
		// DECIMAL neg
		name:        "decimal14_7_neg",
		createType:  "DECIMAL(14,7)",
		createValue: "-4732788.3853794",
	}, {
		// DECIMAL
		name:        "decimal16_8",
		createType:  "DECIMAL(16,8)",
		createValue: "63375311.54574342",
	}, {
		// DECIMAL neg
		name:        "decimal16_8_neg",
		createType:  "DECIMAL(16,8)",
		createValue: "-47327788.38533794",
	}, {
		// DECIMAL
		name:        "decimal18_9",
		createType:  "DECIMAL(18,9)",
		createValue: "633075311.545714342",
	}, {
		// DECIMAL neg
		name:        "decimal18_9_neg",
		createType:  "DECIMAL(18,9)",
		createValue: "-473327788.385033794",
	}, {
		// DECIMAL
		name:        "decimal20_10",
		createType:  "DECIMAL(20,10)",
		createValue: "6330375311.5405714342",
	}, {
		// DECIMAL neg
		name:        "decimal20_10_neg",
		createType:  "DECIMAL(20,10)",
		createValue: "-4731327788.3850337294",
	}, {
		// DECIMAL lots of left digits
		name:        "decimal34_0",
		createType:  "DECIMAL(34,0)",
		createValue: "8765432345678987654345432123456786",
	}, {
		// DECIMAL lots of left digits neg
		name:        "decimal34_0_neg",
		createType:  "DECIMAL(34,0)",
		createValue: "-8765432345678987654345432123456786",
	}, {
		// DECIMAL lots of right digits
		name:        "decimal34_30",
		createType:  "DECIMAL(34,30)",
		createValue: "8765.432345678987654345432123456786",
	}, {
		// DECIMAL lots of right digits neg
		name:        "decimal34_30_neg",
		createType:  "DECIMAL(34,30)",
		createValue: "-8765.432345678987654345432123456786",
	}, {
		// ENUM
		name:        "tshirtsize",
		createType:  "ENUM('x-small', 'small', 'medium', 'large', 'x-larg')",
		createValue: "'large'",
	}, {
		// SET
		name:        "setnumbers",
		createType:  "SET('one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten')",
		createValue: "'two,three,ten'",
	}, {
		// TINYBLOB
		name:        "tiny_blob",
		createType:  "TINYBLOB",
		createValue: "'ab\\'cd'",
	}, {
		// BLOB
		name:        "bloby",
		createType:  "BLOB",
		createValue: "'ab\\'cd'",
	}, {
		// MEDIUMBLOB
		name:        "medium_blob",
		createType:  "MEDIUMBLOB",
		createValue: "'ab\\'cd'",
	}, {
		// LONGBLOB
		name:        "long_blob",
		createType:  "LONGBLOB",
		createValue: "'ab\\'cd'",
	}, {
		// CHAR 8 bits
		name:        "shortchar",
		createType:  "CHAR(30)",
		createValue: "'short char'",
	}, {
		// CHAR 9 bits (100 * 3 = 300, 256<=300<512)
		name:        "mediumchar",
		createType:  "CHAR(100)",
		createValue: "'medium char'",
	}, {
		// CHAR 10 bits (250 * 3 = 750, 512<=750<124)
		name:        "longchar",
		createType:  "CHAR(250)",
		createValue: "'long char'",
	}, {
		// GEOMETRY
		name:        "geo_stuff",
		createType:  "GEOMETRY",
		createValue: "ST_GeomFromText('POINT(1 1)')",
	}}

	conn, f := connectForReplication(t, true /* rbr */)
	defer conn.Close()

	// MariaDB timestamp(N) is not supported by our RBR. See doc.go.
	if !conn.IsMariaDB() {
		testcases = append(testcases, []struct {
			name        string
			createType  string
			createValue string
		}{{
			// TIMESTAMP (second precision)
			name:        "timestamp_second",
			createType:  "TIMESTAMP",
			createValue: "'2012-11-10 15:34:56'",
		}, {
			// TIMESTAMP (100 millisecond precision)
			name:        "timestamp_100millisecond",
			createType:  "TIMESTAMP(1)",
			createValue: "'2012-11-10 15:34:56.6'",
		}, {
			// TIMESTAMP (10 millisecond precision)
			name:        "timestamp_10millisecond",
			createType:  "TIMESTAMP(2)",
			createValue: "'2012-11-10 15:34:56.01'",
		}, {
			// TIMESTAMP (millisecond precision)
			name:        "timestamp_millisecond",
			createType:  "TIMESTAMP(3)",
			createValue: "'2012-11-10 15:34:56.012'",
		}, {
			// TIMESTAMP (100 microsecond precision)
			name:        "timestamp_100microsecond",
			createType:  "TIMESTAMP(4)",
			createValue: "'2012-11-10 15:34:56.0123'",
		}, {
			// TIMESTAMP (10 microsecond precision)
			name:        "timestamp_10microsecond",
			createType:  "TIMESTAMP(5)",
			createValue: "'2012-11-10 15:34:56.01234'",
		}, {
			// TIMESTAMP (microsecond precision)
			name:        "timestamp_microsecond",
			createType:  "TIMESTAMP(6)",
			createValue: "'2012-11-10 15:34:56.012345'",
		}, {
			// TIMESTAMP (0 with microsecond precision)
			name:        "timestamp_microsecond_z",
			createType:  "TIMESTAMP(6)",
			createValue: "'0000-00-00 00:00:00.000000'",
		}, {
			// TIME
			name:        "time_100milli",
			createType:  "TIME(1)",
			createValue: "'12:44:58.3'",
		}, {
			// TIME
			name:        "time_10milli",
			createType:  "TIME(2)",
			createValue: "'412:44:58.01'",
		}, {
			// TIME
			name:        "time_milli",
			createType:  "TIME(3)",
			createValue: "'-12:44:58.012'",
		}, {
			// TIME
			name:        "time_100micro",
			createType:  "TIME(4)",
			createValue: "'12:44:58.0123'",
		}, {
			// TIME
			name:        "time_10micro",
			createType:  "TIME(5)",
			createValue: "'12:44:58.01234'",
		}, {
			// TIME
			name:        "time_micro",
			createType:  "TIME(6)",
			createValue: "'-12:44:58.012345'",
		}, {
			// DATETIME
			name:        "datetime1",
			createType:  "DATETIME(1)",
			createValue: "'1020-08-23 12:44:58.8'",
		}, {
			// DATETIME
			name:        "datetime2",
			createType:  "DATETIME(2)",
			createValue: "'1020-08-23 12:44:58.01'",
		}, {
			// DATETIME
			name:        "datetime3",
			createType:  "DATETIME(3)",
			createValue: "'1020-08-23 12:44:58.012'",
		}, {
			// DATETIME
			name:        "datetime4",
			createType:  "DATETIME(4)",
			createValue: "'1020-08-23 12:44:58.0123'",
		}, {
			// DATETIME
			name:        "datetime5",
			createType:  "DATETIME(5)",
			createValue: "'1020-08-23 12:44:58.01234'",
		}, {
			// DATETIME
			name:        "datetime6",
			createType:  "DATETIME(6)",
			createValue: "'1020-08-23 12:44:58.012345'",
		}}...)
	}

	// JSON is only supported by MySQL 5.7+
	// However the binary format is not just the text version.
	// So it doesn't work as expected.
	if strings.HasPrefix(conn.ServerVersion, "5.7") {
		testcases = append(testcases, []struct {
			name        string
			createType  string
			createValue string
		}{{
			name:        "json1",
			createType:  "JSON",
			createValue: "'{\"a\": 2}'",
		}, {
			name:        "json2",
			createType:  "JSON",
			createValue: "'[1,2]'",
		}, {
			name:        "json3",
			createType:  "JSON",
			createValue: "'{\"a\":\"b\", \"c\":\"d\",\"ab\":\"abc\", \"bc\": [\"x\", \"y\"]}'",
		}, {
			name:        "json4",
			createType:  "JSON",
			createValue: "'[\"here\", [\"I\", \"am\"], \"!!!\"]'",
		}, {
			name:        "json5",
			createType:  "JSON",
			createValue: "'\"scalar string\"'",
		}, {
			name:        "json6",
			createType:  "JSON",
			createValue: "'true'",
		}, {
			name:        "json7",
			createType:  "JSON",
			createValue: "'false'",
		}, {
			name:        "json8",
			createType:  "JSON",
			createValue: "'null'",
		}, {
			name:        "json9",
			createType:  "JSON",
			createValue: "'-1'",
		}, {
			name:        "json10",
			createType:  "JSON",
			createValue: "CAST(CAST(1 AS UNSIGNED) AS JSON)",
		}, {
			name:        "json11",
			createType:  "JSON",
			createValue: "'32767'",
		}, {
			name:        "json12",
			createType:  "JSON",
			createValue: "'32768'",
		}, {
			name:        "json13",
			createType:  "JSON",
			createValue: "'-32768'",
		}, {
			name:        "json14",
			createType:  "JSON",
			createValue: "'-32769'",
		}, {
			name:        "json15",
			createType:  "JSON",
			createValue: "'2147483647'",
		}, {
			name:        "json16",
			createType:  "JSON",
			createValue: "'2147483648'",
		}, {
			name:        "json17",
			createType:  "JSON",
			createValue: "'-2147483648'",
		}, {
			name:        "json18",
			createType:  "JSON",
			createValue: "'-2147483649'",
		}, {
			name:        "json19",
			createType:  "JSON",
			createValue: "'18446744073709551615'",
		}, {
			name:        "json20",
			createType:  "JSON",
			createValue: "'18446744073709551616'",
		}, {
			name:        "json21",
			createType:  "JSON",
			createValue: "'3.14159'",
		}, {
			name:        "json22",
			createType:  "JSON",
			createValue: "'{}'",
		}, {
			name:        "json23",
			createType:  "JSON",
			createValue: "'[]'",
		}, {
			name:        "json24",
			createType:  "JSON",
			createValue: "CAST(CAST('2015-01-15 23:24:25' AS DATETIME) AS JSON)",
		}, {
			name:        "json25",
			createType:  "JSON",
			createValue: "CAST(CAST('23:24:25' AS TIME) AS JSON)",
		}, {
			name:        "json26",
			createType:  "JSON",
			createValue: "CAST(CAST('23:24:25.12' AS TIME(3)) AS JSON)",
		}, {
			name:        "json27",
			createType:  "JSON",
			createValue: "CAST(CAST('2015-01-15' AS DATE) AS JSON)",
		}, {
			name:        "json28",
			createType:  "JSON",
			createValue: "CAST(TIMESTAMP'2015-01-15 23:24:25' AS JSON)",
		}, {
			name:        "json29",
			createType:  "JSON",
			createValue: "CAST(ST_GeomFromText('POINT(1 1)') AS JSON)",
		}, {
			// Decimal has special treatment.
			name:        "json30",
			createType:  "JSON",
			createValue: "CAST(CAST('123456789.1234' AS DECIMAL(13,4)) AS JSON)",
			// FIXME(alainjobart) opaque types are complicated.
			//		}, {
			// This is a bit field. Opaque type in JSON.
			//			name:        "json31",
			//			createType:  "JSON",
			//			createValue: "CAST(x'cafe' AS JSON)",
		}}...)
	}

	ctx := context.Background()
	dConn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}
	defer dConn.Close()

	// Set the connection time zone for execution of the
	// statements to PST. That way we're sure to test the
	// conversion for the TIMESTAMP types.
	if _, err := dConn.ExecuteFetch("SET time_zone = '+08:00'", 0, false); err != nil {
		t.Fatal(err)
	}

	// Create the table with all fields.
	createTable := "create table replicationtypes(id int"
	for _, tcase := range testcases {
		createTable += fmt.Sprintf(", %v %v", tcase.name, tcase.createType)
	}
	createTable += ", primary key(id))"
	if _, err := dConn.ExecuteFetch(createTable, 0, false); err != nil {
		t.Fatal(err)
	}

	// Insert the value with all fields.
	insert := "insert into replicationtypes set id=1"
	for _, tcase := range testcases {
		insert += fmt.Sprintf(", %v=%v", tcase.name, tcase.createValue)
	}
	result, err := dConn.ExecuteFetch(insert, 0, false)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if result.RowsAffected != 1 || len(result.Rows) != 0 {
		t.Errorf("unexpected result for insert: %v", result)
	}

	// Get the new events from the binlogs.
	// Only care about the Write event.
	var tableID uint64
	var tableMap *mysql.TableMap
	var values []sqltypes.Value

	for values == nil {
		data, err := conn.ReadPacket()
		if err != nil {
			t.Fatalf("ReadPacket failed: %v", err)
		}

		// Make sure it's a replication packet.
		switch data[0] {
		case mysql.OKPacket:
			// What we expect, handled below.
		case mysql.ErrPacket:
			err := mysql.ParseErrorPacket(data)
			t.Fatalf("ReadPacket returned an error packet: %v", err)
		default:
			// Very unexpected.
			t.Fatalf("ReadPacket returned a weird packet: %v", data)
		}

		// See what we got, strip the checksum.
		be := conn.MakeBinlogEvent(data[1:])
		if !be.IsValid() {
			t.Fatalf("read an invalid packet: %v", be)
		}
		be, _, err = be.StripChecksum(f)
		if err != nil {
			t.Fatalf("StripChecksum failed: %v", err)
		}
		switch {
		case be.IsTableMap():
			tableID = be.TableID(f) // This would be 0x00ffffff for an event to clear all table map entries.
			var err error
			tableMap, err = be.TableMap(f)
			if err != nil {
				t.Fatalf("TableMap event is broken: %v", err)
			}
			t.Logf("Got Table Map event: %v %v", tableID, tableMap)
			if tableMap.Database != "vttest" ||
				tableMap.Name != "replicationtypes" ||
				len(tableMap.Types) != len(testcases)+1 ||
				tableMap.CanBeNull.Bit(0) {
				t.Errorf("got wrong TableMap: %v", tableMap)
			}
		case be.IsWriteRows():
			if got := be.TableID(f); got != tableID {
				t.Fatalf("WriteRows event got table ID %v but was expecting %v", got, tableID)
			}
			wr, err := be.Rows(f, tableMap)
			if err != nil {
				t.Fatalf("Rows event is broken: %v", err)
			}

			// Check it has the right values
			values, err = valuesForTests(t, &wr, tableMap, 0)
			if err != nil {
				t.Fatalf("valuesForTests is broken: %v", err)
			}
			t.Logf("Got WriteRows event data: %v %v", wr, values)
			if len(values) != len(testcases)+1 {
				t.Fatalf("Got wrong length %v for values, was expecting %v", len(values), len(testcases)+1)
			}

		default:
			t.Logf("Got unrelated event: %v", be)
		}
	}

	// Insert a second row with the same data.
	var sql bytes.Buffer
	sql.WriteString("insert into replicationtypes set id=2")
	for i, tcase := range testcases {
		sql.WriteString(", ")
		sql.WriteString(tcase.name)
		sql.WriteString(" = ")
		if values[i+1].Type() == querypb.Type_TIMESTAMP && !bytes.HasPrefix(values[i+1].ToBytes(), mysql.ZeroTimestamp) {
			// Values in the binary log are UTC. Let's convert them
			// to whatever timezone the connection is using,
			// so MySQL properly converts them back to UTC.
			sql.WriteString("convert_tz(")
			values[i+1].EncodeSQL(&sql)
			sql.WriteString(", '+00:00', @@session.time_zone)")
		} else {
			values[i+1].EncodeSQL(&sql)
		}
	}
	result, err = dConn.ExecuteFetch(sql.String(), 0, false)
	if err != nil {
		t.Fatalf("insert '%v' failed: %v", sql.String(), err)
	}
	if result.RowsAffected != 1 || len(result.Rows) != 0 {
		t.Errorf("unexpected result for insert: %v", result)
	}
	t.Logf("Insert after getting event is: %v", sql.String())

	// Re-select both rows, make sure all columns are the same.
	stmt := "select id"
	for _, tcase := range testcases {
		stmt += ", " + tcase.name
	}
	stmt += " from replicationtypes"
	result, err = dConn.ExecuteFetch(stmt, 2, false)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("unexpected result for select: %v", result)
	}
	for i, tcase := range testcases {
		if !reflect.DeepEqual(result.Rows[0][i+1], result.Rows[1][i+1]) {
			t.Errorf("Field %v is not the same, got %v and %v", tcase.name, result.Rows[0][i+1], result.Rows[1][i+1])
		}
	}

	// Drop the table, we're done.
	if _, err := dConn.ExecuteFetch("drop table replicationtypes", 0, false); err != nil {
		t.Fatal(err)
	}

}

// valuesForTests is a helper method to return the sqltypes.Value
// of all columns in a row in a Row. Only use it in tests, as the
// returned values cannot be interpreted correctly without the schema.
// We assume everything is unsigned in this method.
func valuesForTests(t *testing.T, rs *mysql.Rows, tm *mysql.TableMap, rowIndex int) ([]sqltypes.Value, error) {
	var result []sqltypes.Value

	valueIndex := 0
	data := rs.Rows[rowIndex].Data
	pos := 0
	for c := 0; c < rs.DataColumns.Count(); c++ {
		if !rs.DataColumns.Bit(c) {
			continue
		}

		if rs.Rows[rowIndex].NullColumns.Bit(valueIndex) {
			// This column is represented, but its value is NULL.
			result = append(result, sqltypes.NULL)
			valueIndex++
			continue
		}

		// We have real data
		value, l, err := mysql.CellValue(data, pos, tm.Types[c], tm.Metadata[c], querypb.Type_UINT64)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
		t.Logf("  %v: type=%v data=%v metadata=%v -> %v", c, tm.Types[c], data[pos:pos+l], tm.Metadata[c], value)
		pos += l
		valueIndex++
	}

	return result, nil
}
