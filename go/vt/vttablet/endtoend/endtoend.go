/*
Copyright 2019 The Vitess Authors.

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

// Package endtoend is a test-only package. It runs various
// end-to-end tests on tabletserver.
package endtoend

import (
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
)

func prettyPrint(qr sqltypes.Result) string {
	out, err := json.Marshal(qr)
	if err != nil {
		log.Errorf("Could not marshal result to json for %#v", qr)
		return fmt.Sprintf("%#v", qr)
	}
	return string(out)
}
