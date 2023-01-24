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

package utils

import (
	"fmt"
	"testing"
)

var TestRows = []string{
	"[]",
	"[[INT64(1)]]",
	"[[DECIMAL(6)]]",
	"[[DECIMAL(5)]]",
	"[[DECIMAL(6)]]",
	"[[DECIMAL(8)]]",
	"[[NULL]]",
	"[[INT32(1) INT64(2) INT64(420)]]",
	"[[INT32(1) INT64(2) INT64(420)]]",
	"[[INT32(1) INT64(2) INT64(420)] [INT32(2) INT64(4) INT64(420)] [INT32(3) INT64(6) INT64(420)]]",
	"[[INT64(3) INT64(420)]]",
	"[[INT32(1) INT64(2) INT64(420)]]",
	"[[INT32(1) INT64(2) INT64(420)]]",
	"[[INT64(666) INT64(20) INT64(420)]]",
	"[[INT64(4)]]",
	"[[INT64(12) DECIMAL(7900)]]",
	"[[INT64(3) INT64(4)]]",
	"[[INT32(3)]]",
	"[[INT32(2)]]",
	"[[INT64(3) INT64(4)]]",
	"[[INT32(100) INT64(1) INT64(2)] [INT32(200) INT64(1) INT64(1)] [INT32(300) INT64(1) INT64(1)]]",
	"[[INT64(1) INT64(1)]]",
	"[[INT64(0) INT64(0)]]",
	"[[DECIMAL(2.0000)]]",
	"[[INT32(100) DECIMAL(1.0000)] [INT32(200) DECIMAL(2.0000)] [INT32(300) DECIMAL(3.0000)]]",
	"[[INT64(3) DECIMAL(2.0000)]]",
	"[[INT64(3) INT64(4)]]",
	"[[INT32(100) INT64(1) INT64(2)] [INT32(200) INT64(1) INT64(1)] [INT32(300) INT64(1) INT64(1)]]",
	"[[INT64(1) INT64(1)]]",
	"[[DECIMAL(6)]]",
	"[[FLOAT64(6)]]",
	"[[INT32(3)]]",
	"[[FLOAT64(3)]]",
	"[[INT32(1)]]",
	"[[FLOAT64(1)]]",
	"[[DECIMAL(6) FLOAT64(1)]]",
	"[[INT32(2) DECIMAL(14)]]",
	"[[INT32(3) INT32(9)] [INT32(2) INT32(4)] [INT32(1) INT32(1)]]",
	"[[INT32(3) INT32(9)] [INT32(2) INT32(4)] [INT32(1) INT32(1)]]",
	"[[INT32(1) INT64(20)] [INT32(1) INT64(10)] [INT32(4) INT64(20)] [INT32(2) INT64(10)] [INT32(9) INT64(20)] [INT32(3) INT64(10)]]",
	"[[INT32(2) INT32(4)]]",
	"[[INT32(5) INT32(4)] [INT32(3) INT32(9)] [INT32(2) INT32(4)] [INT32(1) INT32(1)]]",
	"[[INT32(5) INT32(4)] [INT32(3) INT32(9)] [INT32(2) INT32(4)] [INT32(1) INT32(1)]]",
	"[[INT32(2) INT32(4)] [INT32(5) INT32(4)]]",
	"[[INT64(2) DECIMAL(2)] [INT64(1) DECIMAL(0)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(3)]]",
	"[[INT64(1) INT64(3)]]",
	"[[INT64(1) VARCHAR(\"Article 1\") INT64(10)]]",
	"[[INT64(2) VARCHAR(\"Article 2\") INT64(10)]]",
	"[[INT64(1) VARCHAR(\"Article 1\") INT64(10)]]",
	"[[INT64(2) VARCHAR(\"Article 2\") INT64(10)]]",
	"[[VARCHAR(\"albumQ\") INT32(4)] [VARCHAR(\"albumY\") INT32(1)] [VARCHAR(\"albumY\") INT32(2)] [VARCHAR(\"albumX\") INT32(2)] [VARCHAR(\"albumX\") INT32(3)] [VARCHAR(\"albumX\") INT32(1)]]",
	"[[VARCHAR(\"albumQ\") INT32(4)] [VARCHAR(\"albumY\") INT32(1)] [VARCHAR(\"albumY\") INT32(2)] [VARCHAR(\"albumX\") INT32(2)] [VARCHAR(\"albumX\") INT32(3)] [VARCHAR(\"albumX\") INT32(1)]]",
	"[[INT64(2)]]",
	"[[INT32(1) INT32(100)]]",
	"[[INT32(1) INT32(100)]]",
	"[[INT64(2)]]",
	"[[INT64(2)]]",
	"[[INT64(2)]]",
	"[[INT64(2)]]",
	"[[UINT32(70)]]",
	"[[INT64(1) VARCHAR(\"Article 1\") INT64(20)]]",
	"[[INT64(2) VARCHAR(\"Article 2\") INT64(20)]]",
	"[[INT64(1) VARCHAR(\"Article 1\") INT64(20)]]",
	"[[INT64(2) VARCHAR(\"Article 2\") INT64(20)]]",
	"[[INT64(1) VARCHAR(\"Article 1\") INT64(10)]]",
	"[[INT64(2) VARCHAR(\"Article 2\") INT64(10)]]",
	"[]",
	"[]",
	"[[INT64(1) NULL] [INT64(2) INT64(2)]]",
	"[[INT64(1) INT64(1)] [INT64(2) NULL]]",
	"[[INT64(1) INT64(1)]]",
	"[[INT64(1) INT64(8)] [INT64(1) INT64(9)]]",
	"[[INT64(1)] [INT64(2)]]",
	"[[INT64(1)]]",
	"[[INT64(4)] [INT64(8)] [INT64(12)]]",
	"[[INT64(1)]]",
	"[[INT64(1)]]",
	"[[INT64(1)]]",
	"[]",
	"[]",
	"[[INT64(1)]]",
	"[[INT64(1)]]",
	"[[INT64(1)]]",
	"[]",
	"[]",
	"[[INT64(1)]]",
	"[[INT64(1)]]",
	"[[DECIMAL(2) INT64(1)]]",
	"[[NULL INT64(0)]]",
	"[[DECIMAL(420) INT64(1)]]",
	"[[DECIMAL(420) INT64(1)]]",
	"[[NULL INT64(0)]]",
	"[]",
	"[[NULL INT64(0)]]",
	"[]",
	"[[DECIMAL(3) INT64(3)]]",
	"[[DECIMAL(2) INT64(1)] [DECIMAL(1) INT64(1)] [DECIMAL(0) INT64(1)]]",
	"[[NULL INT64(0)]]",
	"[]",
	"[[DECIMAL(423) INT64(4)]]",
	"[[DECIMAL(423) INT64(4)]]",
	"[[DECIMAL(420) INT64(1)] [DECIMAL(2) INT64(1)] [DECIMAL(1) INT64(1)] [DECIMAL(0) INT64(1)]]",
	"[[DECIMAL(420) INT64(1)]]",
	"[[DECIMAL(420) INT64(1)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)] [INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(4)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(1) INT64(2)]]",
	"[[INT64(2) INT64(4)]]",
	"[[INT64(2) INT64(4)]]",
	"[[INT64(1) INT64(1)] [INT64(1) INT64(2)] [INT64(1) INT64(3)]]",
	"[[INT64(1) INT64(1)] [INT64(1) INT64(2)] [INT64(1) INT64(3)] [INT64(1) INT64(4)] [INT64(1) INT64(5)] [INT64(1) INT64(6)]]",
	"[[INT64(1) INT64(1)] [INT64(1) INT64(2)] [INT64(1) INT64(3)]]",
}

func TestRowParsing(t *testing.T) {
	for _, r := range TestRows {
		output, err := ParseRows(r)
		if err != nil {
			t.Errorf("failed to parse %s: %v", r, err)
			continue
		}

		outputstr := fmt.Sprintf("%v", output)
		if r != outputstr {
			t.Errorf("did not rountrip:\ninput:  %s\noutput: %s", r, outputstr)
		}
	}
}
