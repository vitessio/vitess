package replication

import (
	"fmt"
	"reflect"
	"testing"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
)

// sample event data
var (
	garbageEvent       = []byte{92, 93, 211, 208, 16, 71, 139, 255, 83, 199, 198, 59, 148, 214, 109, 154, 122, 226, 39, 41}
	googleRotateEvent  = []byte{0x0, 0x0, 0x0, 0x0, 0x4, 0x88, 0xf3, 0x0, 0x0, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x20, 0x0, 0x23, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x76, 0x74, 0x2d, 0x30, 0x30, 0x30, 0x30, 0x30, 0x36, 0x32, 0x33, 0x34, 0x34, 0x2d, 0x62, 0x69, 0x6e, 0x2e, 0x30, 0x30, 0x30, 0x30, 0x30, 0x31}
	googleFormatEvent  = []byte{0x52, 0x52, 0xe9, 0x53, 0xf, 0x88, 0xf3, 0x0, 0x0, 0x66, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x35, 0x2e, 0x31, 0x2e, 0x36, 0x33, 0x2d, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2d, 0x6c, 0x6f, 0x67, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1b, 0x38, 0xd, 0x0, 0x8, 0x0, 0x12, 0x0, 0x4, 0x4, 0x4, 0x4, 0x12, 0x0, 0x0, 0x53, 0x0, 0x4, 0x1a, 0x8, 0x0, 0x0, 0x0, 0x8, 0x8, 0x8, 0x2}
	googleQueryEvent   = []byte{0x53, 0x52, 0xe9, 0x53, 0x2, 0x88, 0xf3, 0x0, 0x0, 0xad, 0x0, 0x0, 0x0, 0x9a, 0x4, 0x0, 0x0, 0x0, 0x0, 0xb, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x10, 0x0, 0x0, 0x1a, 0x0, 0x0, 0x0, 0x40, 0x0, 0x0, 0x1, 0x0, 0x0, 0x20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x3, 0x73, 0x74, 0x64, 0x4, 0x8, 0x0, 0x8, 0x0, 0x21, 0x0, 0x76, 0x74, 0x5f, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x6b, 0x65, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x0, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x69, 0x66, 0x20, 0x6e, 0x6f, 0x74, 0x20, 0x65, 0x78, 0x69, 0x73, 0x74, 0x73, 0x20, 0x76, 0x74, 0x5f, 0x61, 0x20, 0x28, 0xa, 0x65, 0x69, 0x64, 0x20, 0x62, 0x69, 0x67, 0x69, 0x6e, 0x74, 0x2c, 0xa, 0x69, 0x64, 0x20, 0x69, 0x6e, 0x74, 0x2c, 0xa, 0x70, 0x72, 0x69, 0x6d, 0x61, 0x72, 0x79, 0x20, 0x6b, 0x65, 0x79, 0x28, 0x65, 0x69, 0x64, 0x2c, 0x20, 0x69, 0x64, 0x29, 0xa, 0x29, 0x20, 0x45, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x3d, 0x49, 0x6e, 0x6e, 0x6f, 0x44, 0x42}
	googleXIDEvent     = []byte{0x53, 0x52, 0xe9, 0x53, 0x10, 0x88, 0xf3, 0x0, 0x0, 0x23, 0x0, 0x0, 0x0, 0x4e, 0xa, 0x0, 0x0, 0x0, 0x0, 0xd, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x78, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	googleIntVarEvent1 = []byte{0xea, 0xa8, 0xea, 0x53, 0x5, 0x88, 0xf3, 0x0, 0x0, 0x24, 0x0, 0x0, 0x0, 0xb8, 0x6, 0x0, 0x0, 0x0, 0x0, 0xd, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x65, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	googleIntVarEvent2 = []byte{0xea, 0xa8, 0xea, 0x53, 0x5, 0x88, 0xf3, 0x0, 0x0, 0x24, 0x0, 0x0, 0x0, 0xb8, 0x6, 0x0, 0x0, 0x0, 0x0, 0xd, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x65, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
)

func TestBinlogEventEmptyBuf(t *testing.T) {
	input := binlogEvent([]byte{})
	want := false
	if got := input.IsValid(); got != want {
		t.Errorf("%#v.IsValid() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventGarbage(t *testing.T) {
	input := binlogEvent(garbageEvent)
	want := false
	if got := input.IsValid(); got != want {
		t.Errorf("%#v.IsValid() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsValid(t *testing.T) {
	input := binlogEvent(googleRotateEvent)
	want := true
	if got := input.IsValid(); got != want {
		t.Errorf("%#v.IsValid() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventTruncatedHeader(t *testing.T) {
	input := binlogEvent(googleRotateEvent[:18])
	want := false
	if got := input.IsValid(); got != want {
		t.Errorf("%#v.IsValid() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventTruncatedData(t *testing.T) {
	input := binlogEvent(googleRotateEvent[:len(googleRotateEvent)-1])
	want := false
	if got := input.IsValid(); got != want {
		t.Errorf("%#v.IsValid() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventType(t *testing.T) {
	input := binlogEvent(googleRotateEvent)
	want := byte(0x04)
	if got := input.Type(); got != want {
		t.Errorf("%#v.Type() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventFlags(t *testing.T) {
	input := binlogEvent(googleRotateEvent)
	want := uint16(0x20)
	if got := input.Flags(); got != want {
		t.Errorf("%#v.Flags() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventTimestamp(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := uint32(0x53e95252)
	if got := input.Timestamp(); got != want {
		t.Errorf("%#v.Timestamp() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventServerID(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := uint32(62344)
	if got := input.ServerID(); got != want {
		t.Errorf("%#v.ServerID() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsFormatDescription(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := true
	if got := input.IsFormatDescription(); got != want {
		t.Errorf("%#v.IsFormatDescription() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsNotFormatDescription(t *testing.T) {
	input := binlogEvent(googleRotateEvent)
	want := false
	if got := input.IsFormatDescription(); got != want {
		t.Errorf("%#v.IsFormatDescription() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsQuery(t *testing.T) {
	input := binlogEvent(googleQueryEvent)
	want := true
	if got := input.IsQuery(); got != want {
		t.Errorf("%#v.IsQuery() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsNotQuery(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := false
	if got := input.IsQuery(); got != want {
		t.Errorf("%#v.IsQuery() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsIntVar(t *testing.T) {
	input := binlogEvent(googleIntVarEvent1)
	want := true
	if got := input.IsIntVar(); got != want {
		t.Errorf("%#v.IsIntVar() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsNotIntVar(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := false
	if got := input.IsIntVar(); got != want {
		t.Errorf("%#v.IsIntVar() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsRotate(t *testing.T) {
	input := binlogEvent(googleRotateEvent)
	want := true
	if got := input.IsRotate(); got != want {
		t.Errorf("%#v.IsRotate() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsNotRotate(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := false
	if got := input.IsRotate(); got != want {
		t.Errorf("%#v.IsRotate() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsXID(t *testing.T) {
	input := binlogEvent(googleXIDEvent)
	want := true
	if got := input.IsXID(); got != want {
		t.Errorf("%#v.IsXID() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsNotXID(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := false
	if got := input.IsXID(); got != want {
		t.Errorf("%#v.IsXID() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventFormat(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := BinlogFormat{
		FormatVersion: 4,
		ServerVersion: "5.1.63-google-log",
		HeaderLength:  27,
		HeaderSizes:   googleFormatEvent[76 : len(googleFormatEvent)-5],
	}
	got, err := input.Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%#v.Format() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventFormatWrongVersion(t *testing.T) {
	buf := make([]byte, len(googleFormatEvent))
	copy(buf, googleFormatEvent)
	buf[19] = 5 // mess up the FormatVersion

	input := binlogEvent(buf)
	want := "format version = 5, we only support version 4"
	_, err := input.Format()
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestBinlogEventFormatBadHeaderLength(t *testing.T) {
	buf := make([]byte, len(googleFormatEvent))
	copy(buf, googleFormatEvent)
	buf[19+2+50+4] = 12 // mess up the HeaderLength

	input := binlogEvent(buf)
	want := "header length = 12, should be >= 19"
	_, err := input.Format()
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestBinlogEventQuery(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := binlogEvent(googleQueryEvent)
	want := Query{
		Database: "vt_test_keyspace",
		Charset:  &binlogdatapb.Charset{Client: 8, Conn: 8, Server: 33},
		SQL: `create table if not exists vt_a (
eid bigint,
id int,
primary key(eid, id)
) Engine=InnoDB`,
	}
	got, err := input.Query(f)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%#v.Query() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventQueryBadLength(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	buf := make([]byte, len(googleQueryEvent))
	copy(buf, googleQueryEvent)
	buf[19+8+4+4] = 200 // mess up the db_name length

	input := binlogEvent(buf)
	want := "SQL query position overflows buffer (240 > 146)"
	_, err = input.Query(f)
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestBinlogEventIntVar1(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := binlogEvent(googleIntVarEvent1)
	wantType := byte(IntVarLastInsertID)
	wantValue := uint64(101)
	gotType, gotValue, err := input.IntVar(f)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if gotType != wantType || gotValue != wantValue {
		t.Errorf("%#v.IntVar() = (%#v, %#v), want (%#v, %#v)", input, gotType, gotValue, wantType, wantValue)
	}
}

func TestBinlogEventIntVar2(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := binlogEvent(googleIntVarEvent2)
	wantType := byte(IntVarInsertID)
	wantValue := uint64(101)
	gotType, gotValue, err := input.IntVar(f)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if gotType != wantType || gotValue != wantValue {
		t.Errorf("%#v.IntVar() = (%#v, %#v), want (%#v, %#v)", input, gotType, gotValue, wantType, wantValue)
	}
}

func TestBinlogEventIntVarBadID(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	buf := make([]byte, len(googleIntVarEvent2))
	copy(buf, googleIntVarEvent2)
	buf[19+8] = 3 // mess up the variable ID

	input := binlogEvent(buf)
	want := "invalid IntVar ID: 3"
	_, _, err = input.IntVar(f)
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestCellLengthAndData(t *testing.T) {
	testcases := []struct {
		typ      byte
		metadata uint16
		data     []byte
		out      string
	}{{
		typ:  TypeTiny,
		data: []byte{0x82},
		out:  "130",
	}, {
		typ:  TypeYear,
		data: []byte{0x82},
		out:  "2030",
	}, {
		typ:  TypeShort,
		data: []byte{0x02, 0x01},
		out:  fmt.Sprintf("%v", 0x0102),
	}, {
		typ:  TypeInt24,
		data: []byte{0x03, 0x02, 0x01},
		out:  fmt.Sprintf("%v", 0x010203),
	}, {
		typ:  TypeLong,
		data: []byte{0x04, 0x03, 0x02, 0x01},
		out:  fmt.Sprintf("%v", 0x01020304),
	}, {
		typ:  TypeTimestamp,
		data: []byte{0x84, 0x83, 0x82, 0x81},
		out:  fmt.Sprintf("%v", 0x81828384),
	}, {
		typ:  TypeLongLong,
		data: []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01},
		out:  fmt.Sprintf("%v", 0x0102030405060708),
	}, {
		typ: TypeDate,
		// 2010 << 9 + 10 << 5 + 3 = 1029443 = 0x0fb543
		data: []byte{0x43, 0xb5, 0x0f},
		out:  "2010-10-03",
	}, {
		typ: TypeNewDate,
		// 2010 << 9 + 10 << 5 + 3 = 1029443 = 0x0fb543
		data: []byte{0x43, 0xb5, 0x0f},
		out:  "2010-10-03",
	}, {
		typ: TypeTime,
		// 154532 = 0x00025ba4
		data: []byte{0xa4, 0x5b, 0x02, 0x00},
		out:  "15:45:32",
	}, {
		typ: TypeDateTime,
		// 19840304154532 = 0x120b6e4807a4
		data: []byte{0xa4, 0x07, 0x48, 0x6e, 0x0b, 0x12, 0x00, 0x00},
		out:  "1984-03-04 15:45:32",
	}, {
		typ:      TypeVarchar,
		metadata: 20, // one byte length encoding
		data:     []byte{3, 'a', 'b', 'c'},
		out:      "abc",
	}, {
		typ:      TypeVarchar,
		metadata: 384, // two bytes length encoding
		data:     []byte{3, 0, 'a', 'b', 'c'},
		out:      "abc",
	}, {
		typ:      TypeBit,
		metadata: 0x0107,
		data:     []byte{0x3, 0x1},
		out:      "0000001100000001",
	}, {
		typ:      TypeTimestamp2,
		metadata: 0,
		data:     []byte{0x84, 0x83, 0x82, 0x81},
		out:      fmt.Sprintf("%v", 0x81828384),
	}, {
		typ:      TypeTimestamp2,
		metadata: 1,
		data:     []byte{0x84, 0x83, 0x82, 0x81, 7},
		out:      fmt.Sprintf("%v.7", 0x81828384),
	}, {
		typ:      TypeTimestamp2,
		metadata: 2,
		data:     []byte{0x84, 0x83, 0x82, 0x81, 76},
		out:      fmt.Sprintf("%v.76", 0x81828384),
	}, {
		typ:      TypeTimestamp2,
		metadata: 3,
		// 765 = 0x02fd
		data: []byte{0x84, 0x83, 0x82, 0x81, 0xfd, 0x02},
		out:  fmt.Sprintf("%v.765", 0x81828384),
	}, {
		typ:      TypeTimestamp2,
		metadata: 4,
		// 7654 = 0x1de6
		data: []byte{0x84, 0x83, 0x82, 0x81, 0xe6, 0x1d},
		out:  fmt.Sprintf("%v.7654", 0x81828384),
	}, {
		typ:      TypeTimestamp2,
		metadata: 5,
		// 76543 = 0x012aff
		data: []byte{0x84, 0x83, 0x82, 0x81, 0xff, 0x2a, 0x01},
		out:  fmt.Sprintf("%v.76543", 0x81828384),
	}, {
		typ:      TypeTimestamp2,
		metadata: 6,
		// 765432 = 0x0badf8
		data: []byte{0x84, 0x83, 0x82, 0x81, 0xf8, 0xad, 0x0b},
		out:  fmt.Sprintf("%v.765432", 0x81828384),
	}, {
		typ:      TypeDateTime2,
		metadata: 0,
		// (2012 * 13 + 6) << 22 + 21 << 17 + 15 << 12 + 45 << 6 + 17)
		// = 109734198097 = 0x198caafb51
		// Then have to add 0x8000000000 = 0x998caafb51
		data: []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99},
		out:  "2012-06-21 15:45:17",
	}, {
		typ:      TypeDateTime2,
		metadata: 1,
		data:     []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99, 7},
		out:      "2012-06-21 15:45:17.7",
	}, {
		typ:      TypeDateTime2,
		metadata: 2,
		data:     []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99, 76},
		out:      "2012-06-21 15:45:17.76",
	}, {
		typ:      TypeDateTime2,
		metadata: 3,
		// 765 = 0x02fd
		data: []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99, 0xfd, 0x02},
		out:  "2012-06-21 15:45:17.765",
	}, {
		typ:      TypeDateTime2,
		metadata: 4,
		// 7654 = 0x1de6
		data: []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99, 0xe6, 0x1d},
		out:  "2012-06-21 15:45:17.7654",
	}, {
		typ:      TypeDateTime2,
		metadata: 5,
		// 76543 = 0x012aff
		data: []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99, 0xff, 0x2a, 0x01},
		out:  "2012-06-21 15:45:17.76543",
	}, {
		typ:      TypeDateTime2,
		metadata: 6,
		// 765432 = 0x0badf8
		data: []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99, 0xf8, 0xad, 0x0b},
		out:  "2012-06-21 15:45:17.765432",
	}, {
		// This first set of tests is from a comment in
		//  sql-common/my_time.c:
		//
		// Disk value  intpart frac   Time value   Memory value
		// 800000.00    0      0      00:00:00.00  0000000000.000000
		// 7FFFFF.FF   -1      255   -00:00:00.01  FFFFFFFFFF.FFD8F0
		// 7FFFFF.9D   -1      99    -00:00:00.99  FFFFFFFFFF.F0E4D0
		// 7FFFFF.00   -1      0     -00:00:01.00  FFFFFFFFFF.000000
		// 7FFFFE.FF   -1      255   -00:00:01.01  FFFFFFFFFE.FFD8F0
		// 7FFFFE.F6   -2      246   -00:00:01.10  FFFFFFFFFE.FE7960
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0x00, 0x00, 0x80, 0x00},
		out:      "00:00:00.00",
	}, {
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0xff, 0xff, 0x7f, 0xff},
		out:      "-00:00:00.01",
	}, {
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0xff, 0xff, 0x7f, 0x9d},
		out:      "-00:00:00.99",
	}, {
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0xff, 0xff, 0x7f, 0x00},
		out:      "-00:00:01.00",
	}, {
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0xfe, 0xff, 0x7f, 0xff},
		out:      "-00:00:01.01",
	}, {
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0xfe, 0xff, 0x7f, 0xf6},
		out:      "-00:00:01.10",
	}, {
		// Similar tests for 4 decimals.
		typ:      TypeTime2,
		metadata: 4,
		data:     []byte{0x00, 0x00, 0x80, 0x00, 0x00},
		out:      "00:00:00.0000",
	}, {
		typ:      TypeTime2,
		metadata: 4,
		data:     []byte{0xff, 0xff, 0x7f, 0xff, 0xff},
		out:      "-00:00:00.0001",
	}, {
		typ:      TypeTime2,
		metadata: 4,
		data:     []byte{0xff, 0xff, 0x7f, 0x9d, 0xff},
		out:      "-00:00:00.0099",
	}, {
		typ:      TypeTime2,
		metadata: 4,
		data:     []byte{0xff, 0xff, 0x7f, 0x00, 0x00},
		out:      "-00:00:01.0000",
	}, {
		typ:      TypeTime2,
		metadata: 4,
		data:     []byte{0xfe, 0xff, 0x7f, 0xff, 0xff},
		out:      "-00:00:01.0001",
	}, {
		typ:      TypeTime2,
		metadata: 4,
		data:     []byte{0xfe, 0xff, 0x7f, 0xf6, 0xff},
		out:      "-00:00:01.0010",
	}, {
		// Similar tests for 6 decimals.
		typ:      TypeTime2,
		metadata: 6,
		data:     []byte{0x00, 0x00, 0x80, 0x00, 0x00, 0x00},
		out:      "00:00:00.000000",
	}, {
		typ:      TypeTime2,
		metadata: 6,
		data:     []byte{0xff, 0xff, 0x7f, 0xff, 0xff, 0xff},
		out:      "-00:00:00.000001",
	}, {
		typ:      TypeTime2,
		metadata: 6,
		data:     []byte{0xff, 0xff, 0x7f, 0x9d, 0xff, 0xff},
		out:      "-00:00:00.000099",
	}, {
		typ:      TypeTime2,
		metadata: 6,
		data:     []byte{0xff, 0xff, 0x7f, 0x00, 0x00, 0x00},
		out:      "-00:00:01.000000",
	}, {
		typ:      TypeTime2,
		metadata: 6,
		data:     []byte{0xfe, 0xff, 0x7f, 0xff, 0xff, 0xff},
		out:      "-00:00:01.000001",
	}, {
		typ:      TypeTime2,
		metadata: 6,
		data:     []byte{0xfe, 0xff, 0x7f, 0xf6, 0xff, 0xff},
		out:      "-00:00:01.000010",
	}, {
		// Few more tests.
		typ:      TypeTime2,
		metadata: 0,
		data:     []byte{0x00, 0x00, 0x80},
		out:      "00:00:00",
	}, {
		typ:      TypeTime2,
		metadata: 1,
		data:     []byte{0x01, 0x00, 0x80, 0x0a},
		out:      "00:00:01.1",
	}, {
		typ:      TypeTime2,
		metadata: 2,
		data:     []byte{0x01, 0x00, 0x80, 0x0a},
		out:      "00:00:01.10",
	}, {
		typ:      TypeTime2,
		metadata: 0,
		// 15 << 12 + 34 << 6 + 54 = 63670 = 0x00f8b6
		// and need to add 0x800000
		data: []byte{0xb6, 0xf8, 0x80},
		out:  "15:34:54",
	}}

	for _, tcase := range testcases {
		// Copy the data into a larger buffer (one extra byte
		// on both sides), so we make sure the 'pos' field works.
		padded := make([]byte, len(tcase.data)+2)
		copy(padded[1:], tcase.data)

		// Test cellLength.
		l, err := cellLength(padded, 1, tcase.typ, tcase.metadata)
		if err != nil || l != len(tcase.data) {
			t.Errorf("testcase cellLength(%v,%v) returned unexpected result: %v %v", tcase.typ, tcase.data, l, err)
		}

		// Test cellData (only used for tests, but might as well).
		out, l, err := cellData(padded, 1, tcase.typ, tcase.metadata)
		if err != nil || l != len(tcase.data) || out != tcase.out {
			t.Errorf("testcase cellData(%v,%v) returned unexpected result: %v %v %v, was expecting %v %v <nil>", tcase.typ, tcase.data, out, l, err, tcase.out, len(tcase.data))
		}
	}
}
