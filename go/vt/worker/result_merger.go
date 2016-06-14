package worker

import (
	"fmt"

	"github.com/golang/protobuf/proto"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// ResultMerger returns a sorted stream of multiple ResultReader input streams.
// The output stream will be sorted by ascending primary key order.
// It implements the ResultReader interface.
type ResultMerger struct {
	inputs []ResultReader
	fields []*querypb.Field
}

// NewResultMerger returns a new ResultMerger.
func NewResultMerger(inputs []ResultReader) (*ResultMerger, error) {
	if len(inputs) < 2 {
		panic("ResultMerger requires at least two ResultReaders as input")
	}
	fields := inputs[0].Fields()
	if err := checkFieldsEqual(fields, inputs); err != nil {
		return nil, err
	}

	return &ResultMerger{
		inputs: inputs,
		fields: fields,
	}, nil
}

// Fields returns the field information for the columns in the result.
// It implements the ResultReader interface.
func (rm *ResultMerger) Fields() []*querypb.Field {
	return rm.fields
}

// Next returns the next Result in the sorted, merged stream.
// It implements the ResultReader interface.
func (rm *ResultMerger) Next() (*sqltypes.Result, error) {
	// TODO(mberlin): Implement this function.
	return rm.inputs[0].Next()
}

func checkFieldsEqual(fields []*querypb.Field, inputs []ResultReader) error {
	for i := 1; i < len(inputs); i++ {
		otherFields := inputs[i].Fields()
		if len(fields) != len(otherFields) {
			return fmt.Errorf("input ResultReaders have conflicting Fields data: ResultReader[0]: %v != ResultReader[%d]: %v", fields, i, otherFields)
		}
		for j, field := range fields {
			otherField := otherFields[j]
			if !proto.Equal(field, otherField) {
				return fmt.Errorf("input ResultReaders have conflicting Fields data: ResultReader[0]: %v != ResultReader[%d]: %v", fields, i, otherFields)
			}
		}
	}
	return nil
}
