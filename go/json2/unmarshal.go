// Package json2 provides some improvements over the original json.
package json2

import (
	"bytes"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var carriageReturn = []byte("\n")

// Unmarshal wraps json.Unmarshal, but returns errors that
// also mention the line number. This function is not very
// efficient and should not be used for high QPS operations.
func Unmarshal(data []byte, v interface{}) error {
	if pb, ok := v.(proto.Message); ok {
		return annotate(data, protojson.Unmarshal(data, pb))
	}
	return annotate(data, json.Unmarshal(data, v))
}

func annotate(data []byte, err error) error {
	if err == nil {
		return nil
	}
	syntax, ok := err.(*json.SyntaxError)
	if !ok {
		return err
	}

	start := bytes.LastIndex(data[:syntax.Offset], carriageReturn) + 1
	line, pos := bytes.Count(data[:start], carriageReturn)+1, int(syntax.Offset)-start

	return fmt.Errorf("line: %d, position %d: %v", line, pos, err)
}
