/*
Copyright 2020 The Vitess Authors.

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

package sqlparser

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// ParseTable parses the input as a qualified table name.
// It handles all valid literal escaping.
func ParseTable(input string) (keyspace, table string, err error) {
	tokenizer := NewStringTokenizer(input)

	// Start, want ID
	token, value := tokenizer.Scan()
	switch token {
	case ID:
		table = string(value)
	default:
		table = KeywordString(token)
		if table == "" {
			return "", "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid table name: %s", input)
		}
	}

	// Seen first ID, want '.' or 0
	token, _ = tokenizer.Scan()
	switch token {
	case '.':
		keyspace = table
	case 0:
		return keyspace, table, nil
	default:
		return "", "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid table name: %s", input)
	}

	// Seen '.', want ID
	token, value = tokenizer.Scan()
	switch token {
	case ID:
		table = string(value)
	default:
		table = KeywordString(token)
		if table == "" {
			return "", "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid table name: %s", input)
		}
	}

	// Seen second ID, want 0
	token, _ = tokenizer.Scan()
	switch token {
	case 0:
		return keyspace, table, nil
	default:
		return "", "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid table name: %s", input)
	}
}
