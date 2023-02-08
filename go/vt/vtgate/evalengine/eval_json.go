/*
Copyright 2023 The Vitess Authors.

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

package evalengine

import (
	"errors"
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/json"
)

type errJsonType string

func (fn errJsonType) Error() string {
	return fmt.Sprintf("Invalid data type for JSON data to function %s; a JSON string or JSON type is required.", string(fn))
}

var errJsonPath = errors.New("Invalid JSON path expression.")

type evalJson = json.Value

var _ eval = (*evalJson)(nil)

func intoJson(fn string, e eval) (*evalJson, error) {
	switch e := e.(type) {
	case *evalJson:
		return e, nil
	case *evalBytes:
		var p json.Parser
		return p.ParseBytes(e.bytes)
	default:
		return nil, errJsonType(fn)
	}
}

func intoJsonPath(e eval) (*json.Path, error) {
	switch e := e.(type) {
	case *evalBytes:
		var p json.PathParser
		return p.ParseBytes(e.bytes)
	default:
		return nil, errJsonPath
	}
}
