package vstreamclient

import (
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

// copyRowToStruct builds a struct from a row event
// TODO: this is very rudimentary mapping that only works for top-level fields
func copyRowToStruct(shard shardConfig, row []sqltypes.Value, vPtr reflect.Value) error {
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

			if err = json.Unmarshal(rowVal, structField.Addr().Interface()); err != nil {
				return fmt.Errorf("vstreamclient: error unmarshalling JSON for field %s: %w", fieldName, err)
			}
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
			case reflect.TypeFor[time.Time]():
				if row[m.rowIndex].IsNull() {
					return fmt.Errorf("vstreamclient: error converting row value to time.Time for field %s: NULL into non-pointer field", fieldName)
				}
				rowVal, err := row[m.rowIndex].ToTime(time.UTC) // TODO: make timezone configurable
				if err != nil {
					return fmt.Errorf("vstreamclient: error converting row value to time.Time for field %s: %w", fieldName, err)
				}
				structField.Set(reflect.ValueOf(rowVal))

			default:
				return fmt.Errorf("vstreamclient: unsupported struct field type for field %s: %s", fieldName, m.structType.String())
			}

		case reflect.Pointer,
			reflect.Slice,
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
