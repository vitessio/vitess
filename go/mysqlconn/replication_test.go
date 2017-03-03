package mysqlconn

import (
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/sqldb"
)

func TestComBinlogDump(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Write ComBinlogDump packet, read it, compare.
	if err := cConn.WriteComBinlogDump(0x01020304, "moofarm", 0x05060708, 0x090a); err != nil {
		t.Fatalf("WriteComBinlogDump failed: %v", err)
	}

	data, err := sConn.ReadPacket()
	if err != nil {
		t.Fatalf("sConn.ReadPacket - ComBinlogDump failed: %v", err)
	}

	expectedData := []byte{
		ComBinlogDump,
		0x08, 0x07, 0x06, 0x05, // binlog-pos
		0x0a, 0x09, //flags
		0x04, 0x03, 0x02, 0x01, // server-id
		'm', 'o', 'o', 'f', 'a', 'r', 'm', // binlog-filename
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("ComBinlogDump returned unexpected data:\n%v\nwas expecting:\n%v", data, expectedData)
	}
	sConn.sequence = 0

	// Write ComBinlogDump packet with no filename, read it, compare.
	if err := cConn.WriteComBinlogDump(0x01020304, "", 0x05060708, 0x090a); err != nil {
		t.Fatalf("WriteComBinlogDump failed: %v", err)
	}

	data, err = sConn.ReadPacket()
	if err != nil {
		t.Fatalf("sConn.ReadPacket - ComBinlogDump failed: %v", err)
	}

	expectedData = []byte{
		ComBinlogDump,
		0x08, 0x07, 0x06, 0x05, // binlog-pos
		0x0a, 0x09, // flags
		0x04, 0x03, 0x02, 0x01, // server-id
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("ComBinlogDump returned unexpected data:\n%v\nwas expecting:\n%v", data, expectedData)
	}
}

func TestComBinlogDumpGTID(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Write ComBinlogDumpGTID packet, read it, compare.
	if err := cConn.WriteComBinlogDumpGTID(0x01020304, "moofarm", 0x05060708090a0b0c, 0x0d0e, []byte{0xfa, 0xfb}); err != nil {
		t.Fatalf("WriteComBinlogDumpGTID failed: %v", err)
	}

	data, err := sConn.ReadPacket()
	if err != nil {
		t.Fatalf("sConn.ReadPacket - ComBinlogDumpGTID failed: %v", err)
	}

	expectedData := []byte{
		ComBinlogDumpGTID,
		0x0e, 0x0d, // flags
		0x04, 0x03, 0x02, 0x01, // server-id
		0x07, 0x00, 0x00, 0x00, // binlog-filename-len
		'm', 'o', 'o', 'f', 'a', 'r', 'm', // bilog-filename
		0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, // binlog-pos
		0x02, 0x00, 0x00, 0x00, // data-size
		0xfa, 0xfb, // data
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("ComBinlogDumpGTID returned unexpected data:\n%v\nwas expecting:\n%v", data, expectedData)
	}
	sConn.sequence = 0

	// Write ComBinlogDumpGTID packet with no filename, read it, compare.
	if err := cConn.WriteComBinlogDumpGTID(0x01020304, "", 0x05060708090a0b0c, 0x0d0e, []byte{0xfa, 0xfb}); err != nil {
		t.Fatalf("WriteComBinlogDumpGTID failed: %v", err)
	}

	data, err = sConn.ReadPacket()
	if err != nil {
		t.Fatalf("sConn.ReadPacket - ComBinlogDumpGTID failed: %v", err)
	}

	expectedData = []byte{
		ComBinlogDumpGTID,
		0x0e, 0x0d, // flags
		0x04, 0x03, 0x02, 0x01, // server-id
		0x00, 0x00, 0x00, 0x00, // binlog-filename-len
		0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, // binlog-pos
		0x02, 0x00, 0x00, 0x00, // data-size
		0xfa, 0xfb, // data
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("ComBinlogDumpGTID returned unexpected data:\n%v\nwas expecting:\n%v", data, expectedData)
	}
}

// connectForReplication is a helper method to connect for replication
// from the current binlog position.
func connectForReplication(t *testing.T, params *sqldb.ConnParams, rbr bool) (*Conn, bool, replication.BinlogFormat) {
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatal(err)
	}

	// We need to know if this is MariaDB.
	isMariaDB := false
	if strings.Contains(strings.ToLower(conn.ServerVersion), "mariadb") {
		isMariaDB = true

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
	file := result.Rows[0][0].String()
	position, err := result.Rows[0][1].ParseUint64()
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
	var f replication.BinlogFormat
	for {
		data, err := conn.ReadPacket()
		if err != nil {
			t.Fatalf("ReadPacket failed: %v", err)
		}

		// Make sure it's a replication packet.
		switch data[0] {
		case OKPacket:
			// What we expect, handled below.
		case ErrPacket:
			err := parseErrorPacket(data)
			t.Fatalf("ReadPacket returned an error packet: %v", err)
		default:
			// Very unexpected.
			t.Fatalf("ReadPacket returned a weird packet: %v", data)
		}

		// See what we got.
		be := newBinlogEvent(isMariaDB, data)
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

	return conn, isMariaDB, f
}

// newBinlogEvent uses the flavor to create the right packet.
func newBinlogEvent(isMariaDB bool, data []byte) replication.BinlogEvent {
	// Hardcoded test for MySQL server version.
	if isMariaDB {
		return replication.NewMariadbBinlogEvent(data[1:])
	}
	return replication.NewMysql56BinlogEvent(data[1:])
}

// testReplicationConnectionClosing connects as a replication client,
// gets the first packet, then waits a few milliseconds and closes the
// connection. We should get the right error.
func testReplicationConnectionClosing(t *testing.T, params *sqldb.ConnParams) {
	conn, _, _ := connectForReplication(t, params, false /* rbr */)
	defer conn.Close()

	// One go routine is waiting on events.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			data, err := conn.ReadPacket()
			if err != nil {
				serr, ok := err.(*sqldb.SQLError)
				if !ok {
					t.Fatalf("Got a non sqldb.SQLError error: %v", err)
				}
				if serr.Num != CRServerLost {
					t.Fatalf("Got an unexpected sqldb.SQLError error: %v", serr)
				}
				// we got the right error, all good.
				return
			}

			// Make sure it's a replication packet.
			switch data[0] {
			case OKPacket:
				// What we expect, keep going.
			case ErrPacket:
				err := parseErrorPacket(data)
				t.Fatalf("ReadPacket returned an error packet: %v", err)
			default:
				// Very unexpected.
				t.Fatalf("ReadPacket returned a weird packet: %v", data)
			}
		}
	}()

	// Connect and create a table.
	ctx := context.Background()
	dConn, err := Connect(ctx, params)
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

func testStatementReplicationWithRealDatabase(t *testing.T, params *sqldb.ConnParams) {
	conn, isMariaDB, f := connectForReplication(t, params, false /* rbr */)
	defer conn.Close()

	// Create a table, insert some data in it.
	ctx := context.Background()
	dConn, err := Connect(ctx, params)
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
		case OKPacket:
			// What we expect, handled below.
		case ErrPacket:
			err := parseErrorPacket(data)
			t.Fatalf("ReadPacket returned an error packet: %v", err)
		default:
			// Very unexpected.
			t.Fatalf("ReadPacket returned a weird packet: %v", data)
		}

		// See what we got, strip the checksum.
		be := newBinlogEvent(isMariaDB, data)
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

func testRowReplicationWithRealDatabase(t *testing.T, params *sqldb.ConnParams) {
	conn, isMariaDB, f := connectForReplication(t, params, true /* rbr */)
	defer conn.Close()

	// Create a table, insert some data in it.
	ctx := context.Background()
	dConn, err := Connect(ctx, params)
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
	var tableMap *replication.TableMap

	//	for i := 0; i < 6 && (gtidCount < 2 || !gotCreateTable || !gotTableMapEvent || !gotBegin || !gotInsert || !gotCommit); i++ {
	for gtidCount < 4 || !gotCreateTable || !gotTableMapEvent || !gotInsert || !gotUpdate || !gotDelete || beginCount != 3 || commitCount != 3 {
		data, err := conn.ReadPacket()
		if err != nil {
			t.Fatalf("ReadPacket failed: %v", err)
		}

		// Make sure it's a replication packet.
		switch data[0] {
		case OKPacket:
			// What we expect, handled below.
		case ErrPacket:
			err := parseErrorPacket(data)
			t.Fatalf("ReadPacket returned an error packet: %v", err)
		default:
			// Very unexpected.
			t.Fatalf("ReadPacket returned a weird packet: %v", data)
		}

		// See what we got, strip the checksum.
		be := newBinlogEvent(isMariaDB, data)
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
				len(tableMap.Columns) != 2 ||
				tableMap.Columns[0].CanBeNull ||
				!tableMap.Columns[1].CanBeNull {
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
			values, _ := wr.StringValues(tableMap, 0)
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
			values, _ := ur.StringIdentifies(tableMap, 0)
			t.Logf("Got UpdateRows event identify: %v %v", ur, values)
			if expected := []string{"10", "nice name"}; !reflect.DeepEqual(values, expected) {
				t.Fatalf("StringIdentifies returned %v, expected %v", values, expected)
			}

			// Check it has 2 values rows, and first value is '10', second value is 'nicer name'.
			values, _ = ur.StringValues(tableMap, 0)
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
			values, _ := dr.StringIdentifies(tableMap, 0)
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
