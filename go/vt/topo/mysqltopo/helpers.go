/*
Copyright 2025 The Vitess Authors.

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

package mysqltopo

import (
	"encoding/hex"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
)

// expandQuery converts a query + arguments into a properly formatted SQL query.
// It uses sqltypes.EncodeStringSQL to ensure all arguments are safely escaped.
func expandQuery(query string, args ...string) string {
	if len(args) > 0 {
		// Encode all arguments using sqltypes.EncodeStringSQL
		encodedArgs := make([]interface{}, len(args))
		for i, arg := range args {
			encodedArgs[i] = sqltypes.EncodeStringSQL(arg)
		}
		query = fmt.Sprintf(query, encodedArgs...)
	}
	return query
}

// encodeBinaryData encodes binary data as a hex string for MySQL storage.
// This ensures that binary protobuf data is stored correctly without corruption.
func encodeBinaryData(data []byte) string {
	if len(data) == 0 {
		return "0x" // Empty hex literal
	}
	// Use MySQL's hex literal format: 0x<hexdata>
	return "0x" + hex.EncodeToString(data)
}

// decodeBinaryData decodes hex-encoded binary data from MySQL storage.
// This reverses the encoding done by encodeBinaryData.
func decodeBinaryData(value sqltypes.Value) ([]byte, error) {
	if value.IsNull() {
		return nil, nil
	}

	data, err := value.ToBytes()
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	// Check if it's hex-encoded (starts with "0x")
	str := string(data)
	if len(str) >= 2 && str[:2] == "0x" {
		// Decode hex string
		return hex.DecodeString(str[2:])
	}

	// If not hex-encoded, return as-is (for backward compatibility)
	return data, nil
}
