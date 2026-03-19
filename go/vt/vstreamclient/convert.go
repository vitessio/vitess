package vstreamclient

import (
	"database/sql"
	"database/sql/driver"
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// VStreamScanner allows for custom scan implementations
type VStreamScanner interface {
	VStreamScan(fields []*querypb.Field, row []sqltypes.Value, rowEvent *binlogdatapb.RowEvent, rowChange *binlogdatapb.RowChange) error
}

var (
	timeType              = reflect.TypeFor[time.Time]()
	sqlScannerType        = reflect.TypeFor[sql.Scanner]()
	textUnmarshalerType   = reflect.TypeFor[encoding.TextUnmarshaler]()
	byteSliceElemKind     = reflect.Uint8
	defaultDecodeLocation = time.UTC
)

// copyRowToStruct builds a struct from a row event.
func copyRowToStruct(shard shardConfig, row []sqltypes.Value, vPtr reflect.Value) error {
	return copyRowToStructInLocation(shard, row, vPtr, defaultDecodeLocation)
}

func copyRowToStructInLocation(shard shardConfig, row []sqltypes.Value, vPtr reflect.Value, loc *time.Location) error {
	if loc == nil {
		loc = defaultDecodeLocation
	}

	for fieldName, m := range shard.fieldMap {
		structField := reflect.Indirect(vPtr).FieldByIndex(m.structIndex)

		// Handle pointer fields by allocating as needed.
		// NULL values are left as nil pointers.
		if m.isPointer {
			if row[m.rowIndex].IsNull() {
				structField.SetZero()
				continue
			}
			if structField.IsNil() {
				structField.Set(reflect.New(m.structType))
			}
			structField = structField.Elem()
		}

		if m.jsonDecode {
			if row[m.rowIndex].IsNull() {
				return fmt.Errorf("vstreamclient: error unmarshalling JSON for field %s: NULL into non-pointer field", fieldName)
			}

			rowVal, err := row[m.rowIndex].ToBytes()
			if err != nil {
				return fmt.Errorf("vstreamclient: error converting row value to bytes for field %s: %w", fieldName, err)
			}
			if isByteSliceType(structField.Type()) {
				setByteSliceValue(structField, rowVal)
				continue
			}

			if err = json.Unmarshal(rowVal, structField.Addr().Interface()); err != nil {
				return fmt.Errorf("vstreamclient: error unmarshalling JSON for field %s: %w", fieldName, err)
			}
			continue
		}

		handled, err := tryScanSpecialField(structField, row[m.rowIndex], loc)
		if err != nil {
			return fmt.Errorf("vstreamclient: error converting row value for field %s: %w", fieldName, err)
		}
		if handled {
			continue
		}

		switch m.kind {
		case reflect.Bool:
			if row[m.rowIndex].IsNull() {
				return fmt.Errorf("vstreamclient: error converting row value to bool for field %s: NULL into non-pointer field", fieldName)
			}
			rowVal, err := row[m.rowIndex].ToBool()
			if err != nil {
				return fmt.Errorf("vstreamclient: error converting row value to bool for field %s: %w", fieldName, err)
			}
			structField.SetBool(rowVal)

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if row[m.rowIndex].IsNull() {
				return fmt.Errorf("vstreamclient: error converting row value to int64 for field %s: NULL into non-pointer field", fieldName)
			}
			rowVal, err := row[m.rowIndex].ToInt64()
			if err != nil {
				return fmt.Errorf("vstreamclient: error converting row value to int64 for field %s: %w", fieldName, err)
			}
			if structField.OverflowInt(rowVal) {
				return fmt.Errorf("vstreamclient: error converting row value to int64 for field %s: %d overflows destination type %s", fieldName, rowVal, structField.Type())
			}
			structField.SetInt(rowVal)

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if row[m.rowIndex].IsNull() {
				return fmt.Errorf("vstreamclient: error converting row value to uint64 for field %s: NULL into non-pointer field", fieldName)
			}
			rowVal, err := row[m.rowIndex].ToUint64()
			if err != nil {
				return fmt.Errorf("vstreamclient: error converting row value to uint64 for field %s: %w", fieldName, err)
			}
			if structField.OverflowUint(rowVal) {
				return fmt.Errorf("vstreamclient: error converting row value to uint64 for field %s: %d overflows destination type %s", fieldName, rowVal, structField.Type())
			}
			structField.SetUint(rowVal)

		case reflect.Float32, reflect.Float64:
			if row[m.rowIndex].IsNull() {
				return fmt.Errorf("vstreamclient: error converting row value to float64 for field %s: NULL into non-pointer field", fieldName)
			}
			rowVal, err := row[m.rowIndex].ToFloat64()
			if err != nil {
				return fmt.Errorf("vstreamclient: error converting row value to float64 for field %s: %w", fieldName, err)
			}
			if structField.OverflowFloat(rowVal) {
				return fmt.Errorf("vstreamclient: error converting row value to float64 for field %s: %v overflows destination type %s", fieldName, rowVal, structField.Type())
			}
			structField.SetFloat(rowVal)

		case reflect.String:
			if row[m.rowIndex].IsNull() {
				return fmt.Errorf("vstreamclient: error converting row value to string for field %s: NULL into non-pointer field", fieldName)
			}
			rowVal := row[m.rowIndex].ToString()
			structField.SetString(rowVal)

		case reflect.Struct:
			switch m.structType {
			case timeType:
				if row[m.rowIndex].IsNull() {
					return fmt.Errorf("vstreamclient: error converting row value to time.Time for field %s: NULL into non-pointer field", fieldName)
				}
				rowVal, err := row[m.rowIndex].ToTime(loc)
				if err != nil {
					return fmt.Errorf("vstreamclient: error converting row value to time.Time for field %s: %w", fieldName, err)
				}
				structField.Set(reflect.ValueOf(rowVal))

			default:
				return fmt.Errorf("vstreamclient: unsupported struct field type for field %s: %s", fieldName, m.structType.String())
			}

		case reflect.Slice:
			if !isByteSliceType(m.structType) {
				return fmt.Errorf("vstreamclient: unsupported field type: %s", m.kind.String())
			}
			if row[m.rowIndex].IsNull() {
				return fmt.Errorf("vstreamclient: error converting row value to bytes for field %s: NULL into non-pointer field", fieldName)
			}
			rowVal, err := row[m.rowIndex].ToBytes()
			if err != nil {
				return fmt.Errorf("vstreamclient: error converting row value to bytes for field %s: %w", fieldName, err)
			}
			setByteSliceValue(structField, rowVal)

		case reflect.Pointer,
			reflect.Array,
			reflect.Invalid,
			reflect.Uintptr,
			reflect.Complex64,
			reflect.Complex128,
			reflect.Chan,
			reflect.Func,
			reflect.Interface,
			reflect.Map,
			reflect.UnsafePointer:
			return fmt.Errorf("vstreamclient: unsupported field type: %s", m.kind.String())
		}
	}

	return nil
}

func tryScanSpecialField(structField reflect.Value, rowValue sqltypes.Value, loc *time.Location) (bool, error) {
	if structField.Type() == timeType {
		return false, nil
	}

	if structField.CanAddr() {
		if structField.Addr().Type().Implements(sqlScannerType) {
			scanner := structField.Addr().Interface().(sql.Scanner)
			driverValue, err := sqlValueToDriverValue(rowValue, loc)
			if err != nil {
				return true, err
			}
			return true, scanner.Scan(driverValue)
		}

		if structField.Addr().Type().Implements(textUnmarshalerType) {
			if rowValue.IsNull() {
				return false, nil
			}
			rowBytes, err := rowValue.ToBytes()
			if err != nil {
				return true, err
			}
			unmarshaler := structField.Addr().Interface().(encoding.TextUnmarshaler)
			return true, unmarshaler.UnmarshalText(rowBytes)
		}
	}

	return false, nil
}

func sqlValueToDriverValue(rowValue sqltypes.Value, loc *time.Location) (driver.Value, error) {
	if rowValue.IsNull() {
		return nil, nil
	}

	if loc == nil {
		loc = defaultDecodeLocation
	}

	switch rowValue.Type() {
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64:
		return rowValue.ToInt64()

	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32:
		rowVal, err := rowValue.ToUint64()
		if err != nil {
			return nil, err
		}
		return int64(rowVal), nil

	case sqltypes.Float32, sqltypes.Float64:
		return rowValue.ToFloat64()

	case sqltypes.Decimal:
		return rowValue.ToString(), nil

	case sqltypes.Date, sqltypes.Datetime, sqltypes.Timestamp:
		return rowValue.ToTime(loc)
	}

	if rowVal, err := rowValue.ToBool(); err == nil {
		return rowVal, nil
	}

	return rowValue.ToBytes()
}

func isByteSliceType(t reflect.Type) bool {
	return t.Kind() == reflect.Slice && t.Elem().Kind() == byteSliceElemKind
}

func setByteSliceValue(structField reflect.Value, rowVal []byte) {
	cloned := append([]byte(nil), rowVal...)
	structField.Set(reflect.ValueOf(cloned).Convert(structField.Type()))
}
