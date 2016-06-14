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
	input  []ResultReader
	fields []*querypb.Field
}

// NewResultMerger returns a new ResultMerger.
func NewResultMerger(input []ResultReader) (*ResultMerger, error) {
	if len(input) < 1 {
		panic("list of input ResultReader is empty")
	}
	fields := input[0].Fields()
	if err := checkFieldsEqual(fields, input); err != nil {
		return nil, err
	}

	return &ResultMerger{
		input:  input,
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
	return rm.input[0].Next()
}

func checkFieldsEqual(fields []*querypb.Field, input []ResultReader) error {
	for i := 1; i < len(input); i++ {
		otherFields := input[i].Fields()
		if len(fields) != len(otherFields) {
			return fmt.Errorf("input ResultReader have conflicting Fields data: ResultReader[0]: %v != ResultReader[%d]: %v", fields, i, otherFields)
		}
		for j, field := range fields {
			otherField := otherFields[j]
			if !proto.Equal(field, otherField) {
				return fmt.Errorf("input ResultReader have conflicting Fields data: ResultReader[0]: %v != ResultReader[%d]: %v", fields, i, otherFields)
			}
		}
	}
	return nil
}
