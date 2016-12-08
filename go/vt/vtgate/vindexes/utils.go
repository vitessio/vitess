package vindexes

import (
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// parseString takes a string and tries to extract a number from it.
func parseString(s string) (int64, error) {
	signed, err := strconv.ParseInt(s, 0, 64)
	if err == nil {
		return signed, nil
	}
	unsigned, err := strconv.ParseUint(s, 0, 64)
	if err == nil {
		return int64(unsigned), nil
	}
	return 0, fmt.Errorf("parseString: %v", err)
}

// parseValue handles the type + value parameters.
func parseValue(typ querypb.Type, val []byte) (int64, error) {
	if sqltypes.IsSigned(typ) {
		signed, err := strconv.ParseInt(string(val), 0, 64)
		if err != nil {
			return 0, fmt.Errorf("parseValue: %v", err)
		}
		return signed, nil
	}
	if sqltypes.IsUnsigned(typ) {
		unsigned, err := strconv.ParseUint(string(val), 0, 64)
		if err != nil {
			return 0, fmt.Errorf("parseValue: %v", err)
		}
		return int64(unsigned), nil
	}
	if sqltypes.IsText(typ) || sqltypes.IsBinary(typ) {
		return parseString(string(val))
	}
	return 0, fmt.Errorf("parseValue: incompatible type %v", typ)
}

// getNumber extracts a number from a bind variable.
// It handles most bind variable types gracefully.
func getNumber(v interface{}) (int64, error) {
	switch v := v.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case []byte:
		return parseString(string(v))
	case string:
		return parseString(v)
	case sqltypes.Value:
		return parseValue(v.Type(), v.Raw())
	case *querypb.BindVariable:
		return parseValue(v.Type, v.Value)
	}
	return 0, fmt.Errorf("getNumber: unexpected type for %v: %T", v, v)
}

// getBytes returns the raw bytes for a value.
func getBytes(key interface{}) ([]byte, error) {
	switch v := key.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case sqltypes.Value:
		return v.Raw(), nil
	case *querypb.BindVariable:
		return v.Value, nil
	}
	return nil, fmt.Errorf("unexpected data type for getBytes: %T", key)
}
