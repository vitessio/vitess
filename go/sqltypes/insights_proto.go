/*
Copyright 2022 The Vitess Authors.

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

package sqltypes

import (
	"encoding/json"
	"strings"
)

type OKInfoJSON struct {
	RowsRead uint64 `json:"rows_read"`
}

func parseOKInfoJSON(info string) (out OKInfoJSON) {
	if info == "" {
		return
	}
	tok := strings.Split(info, "\000")
	if len(tok) < 2 {
		return
	}

	json.Unmarshal([]byte(tok[1]), &out)

	// in case of unmarshal error or value not present, this returns all 0 values
	return
}
