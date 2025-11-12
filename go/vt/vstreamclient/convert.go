package vstreamclient

import (
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

// copyRowToStruct builds a customer from a row event
// TODO: this is very rudimentary mapping that only works for top-level fields
func copyRowToStruct(shard shardConfig, row []sqltypes.Value, vPtr reflect.Value) error {
	for fieldName, m := range shard.fieldMap {
		structField := reflect.Indirect(vPtr).FieldByIndex(m.structIndex)

		switch m.kind {
		case reflect.Bool:
			rowVal, err := row[m.rowIndex].ToBool()
			if err != nil {
				return fmt.Errorf("error converting row value to bool for field %s: %w", fieldName, err)
			}
			structField.SetBool(rowVal)

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			rowVal, err := row[m.rowIndex].ToInt64()
			if err != nil {
				return fmt.Errorf("error converting row value to int64 for field %s: %w", fieldName, err)
			}
			structField.SetInt(rowVal)

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			rowVal, err := row[m.rowIndex].ToUint64()
			if err != nil {
				return fmt.Errorf("error converting row value to uint64 for field %s: %w", fieldName, err)
			}
			structField.SetUint(rowVal)

		case reflect.Float32, reflect.Float64:
			rowVal, err := row[m.rowIndex].ToFloat64()
			if err != nil {
				return fmt.Errorf("error converting row value to float64 for field %s: %w", fieldName, err)
			}
			structField.SetFloat(rowVal)

		case reflect.String:
			rowVal := row[m.rowIndex].ToString()
			structField.SetString(rowVal)

		case reflect.Struct:
			switch m.structType.(type) {
			case time.Time, *time.Time:
				rowVal, err := row[m.rowIndex].ToTime(time.UTC) // TODO: make timezone configurable
				if err != nil {
					return fmt.Errorf("error converting row value to time.Time for field %s: %w", fieldName, err)
				}
				structField.Set(reflect.ValueOf(rowVal))
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
