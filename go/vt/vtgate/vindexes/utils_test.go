package vindexes

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestGetNumber(t *testing.T) {
	tcases := []struct {
		in  interface{}
		out string
	}{{
		in:  []byte{'1', '2', '3'},
		out: "123",
	}, {
		in:  sqltypes.MakeTrusted(querypb.Type_UINT64, []byte{'1', '2', '3'}),
		out: "123",
	}, {
		in:  int(12),
		out: "12",
	}, {
		in:  int8(12),
		out: "12",
	}, {
		in:  int16(12),
		out: "12",
	}, {
		in:  int32(12),
		out: "12",
	}, {
		in:  int64(-12),
		out: "-12",
	}, {
		in:  uint(125),
		out: "125",
	}, {
		in:  uint8(125),
		out: "125",
	}, {
		in:  uint16(125),
		out: "125",
	}, {
		in:  uint32(124),
		out: "124",
	}, {
		in:  uint64(123),
		out: "123",
	}, {
		in:  "1234",
		out: "1234",
	}, {
		in:  "18446744073709551606", // 2^64-10
		out: "-10",
	}, {
		in:  "not a number",
		out: `parseString: strconv.ParseUint: parsing "not a number": invalid syntax`,
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_INT64,
			Value: []byte{'-', '1', '2', '3'},
		},
		out: "-123",
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_UINT64,
			Value: []byte{'1', '2', '3'},
		},
		out: "123",
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_VARCHAR,
			Value: []byte{'1', '2', '3'},
		},
		out: "123",
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_VARBINARY,
			Value: []byte{'1', '2', '3'},
		},
		out: "123",
	}, {
		in: &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
		},
		out: "parseValue: incompatible type TUPLE",
	}}
	for _, tcase := range tcases {
		n, err := getNumber(tcase.in)
		got := ""
		if err != nil {
			got = err.Error()
		} else {
			got = fmt.Sprintf("%v", n)
		}
		if got != tcase.out {
			t.Errorf("getNumber(%v) got %v %v expected %v", tcase.in, n, err, tcase.out)
		}
	}
}

func TestGetBytes(t *testing.T) {
	tcases := []struct {
		in  interface{}
		out string
	}{{
		in:  []byte{'1', '2', '3'},
		out: "123",
	}, {
		in:  "1234",
		out: "1234",
	}, {
		in:  sqltypes.MakeTrusted(querypb.Type_UINT64, []byte{'1', '2', '3'}),
		out: "123",
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_VARBINARY,
			Value: []byte{'1', '2', '3'},
		},
		out: "123",
	}, {
		in:  65,
		out: "unexpected data type for getBytes: int",
	}}
	for _, tcase := range tcases {
		b, err := getBytes(tcase.in)
		got := ""
		if err != nil {
			got = err.Error()
		} else {
			got = string(b)
		}
		if got != tcase.out {
			t.Errorf("getBytes(%v) got %v %v expected %v", tcase.in, b, err, tcase.out)
		}
	}
}
