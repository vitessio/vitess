/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package json2 provides some improvements over the original json.
package json2

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
)

var carriageReturn = []byte("\n")

// Unmarshal wraps json.Unmarshal, but returns errors that
// also mention the line number. This function is not very
// efficient and should not be used for high QPS operations.
func Unmarshal(data []byte, v interface{}) error {
	if pb, ok := v.(proto.Message); ok {
		return annotate(data, jsonpb.Unmarshal(bytes.NewBuffer(data), pb))
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
