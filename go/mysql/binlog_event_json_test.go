/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	"strings"
	"testing"
)

func TestJSON(t *testing.T) {
	testcases := []struct {
		data     []byte
		expected string
	}{{
		data:     []byte{},
		expected: `'null'`,
	}, {
		data:     []byte{0, 1, 0, 14, 0, 11, 0, 1, 0, 12, 12, 0, 97, 1, 98},
		expected: `JSON_OBJECT('a','b')`,
	}, {
		data:     []byte{0, 1, 0, 12, 0, 11, 0, 1, 0, 5, 2, 0, 97},
		expected: `JSON_OBJECT('a',2)`,
	}, {
		data:     []byte{2, 2, 0, 10, 0, 5, 1, 0, 5, 2, 0},
		expected: `JSON_ARRAY(1,2)`,
	}, {
		data:     []byte{0, 4, 0, 60, 0, 32, 0, 1, 0, 33, 0, 1, 0, 34, 0, 2, 0, 36, 0, 2, 0, 12, 38, 0, 12, 40, 0, 12, 42, 0, 2, 46, 0, 97, 99, 97, 98, 98, 99, 1, 98, 1, 100, 3, 97, 98, 99, 2, 0, 14, 0, 12, 10, 0, 12, 12, 0, 1, 120, 1, 121},
		expected: `JSON_OBJECT('a','b','c','d','ab','abc','bc',JSON_ARRAY('x','y'))`,
	}, {
		data:     []byte{2, 3, 0, 37, 0, 12, 13, 0, 2, 18, 0, 12, 33, 0, 4, 104, 101, 114, 101, 2, 0, 15, 0, 12, 10, 0, 12, 12, 0, 1, 73, 2, 97, 109, 3, 33, 33, 33},
		expected: `JSON_ARRAY('here',JSON_ARRAY('I','am'),'!!!')`,
	}, {
		data:     []byte{12, 13, 115, 99, 97, 108, 97, 114, 32, 115, 116, 114, 105, 110, 103},
		expected: `'"scalar string"'`,
	}, {
		data:     []byte{4, 1},
		expected: `'true'`,
	}, {
		data:     []byte{4, 2},
		expected: `'false'`,
	}, {
		data:     []byte{4, 0},
		expected: `'null'`,
	}, {
		data:     []byte{5, 255, 255},
		expected: `'-1'`,
	}, {
		data:     []byte{6, 1, 0},
		expected: `'1'`,
	}, {
		data:     []byte{5, 255, 127},
		expected: `'32767'`,
	}, {
		data:     []byte{7, 0, 128, 0, 0},
		expected: `'32768'`,
	}, {
		data:     []byte{5, 0, 128},
		expected: `'-32768'`,
	}, {
		data:     []byte{7, 255, 127, 255, 255},
		expected: `'-32769'`,
	}, {
		data:     []byte{7, 255, 255, 255, 127},
		expected: `'2147483647'`,
	}, {
		data:     []byte{9, 0, 0, 0, 128, 0, 0, 0, 0},
		expected: `'2147483648'`,
	}, {
		data:     []byte{7, 0, 0, 0, 128},
		expected: `'-2147483648'`,
	}, {
		data:     []byte{9, 255, 255, 255, 127, 255, 255, 255, 255},
		expected: `'-2147483649'`,
	}, {
		data:     []byte{10, 255, 255, 255, 255, 255, 255, 255, 255},
		expected: `'18446744073709551615'`,
	}, {
		data:     []byte{9, 0, 0, 0, 0, 0, 0, 0, 128},
		expected: `'-9223372036854775808'`,
	}, {
		data:     []byte{11, 110, 134, 27, 240, 249, 33, 9, 64},
		expected: `'3.14159E+00'`,
	}, {
		data:     []byte{0, 0, 0, 4, 0},
		expected: `JSON_OBJECT()`,
	}, {
		data:     []byte{2, 0, 0, 4, 0},
		expected: `JSON_ARRAY()`,
	}, {
		// opaque, datetime
		data:     []byte{15, 12, 8, 0, 0, 0, 25, 118, 31, 149, 25},
		expected: `CAST(CAST('2015-01-15 23:24:25' AS DATETIME(6)) AS JSON)`,
	}, {
		// opaque, time
		data:     []byte{15, 11, 8, 0, 0, 0, 25, 118, 1, 0, 0},
		expected: `CAST(CAST('23:24:25' AS TIME(6)) AS JSON)`,
	}, {
		// opaque, time
		data:     []byte{15, 11, 8, 192, 212, 1, 25, 118, 1, 0, 0},
		expected: `CAST(CAST('23:24:25.120000' AS TIME(6)) AS JSON)`,
	}, {
		// opaque, date
		data:     []byte{15, 10, 8, 0, 0, 0, 0, 0, 30, 149, 25},
		expected: `CAST(CAST('2015-01-15' AS DATE) AS JSON)`,
	}, {
		// opaque, decimal
		data:     []byte{15, 246, 8, 13, 4, 135, 91, 205, 21, 4, 210},
		expected: `CAST(CAST('123456789.1234' AS DECIMAL(13,4)) AS JSON)`,
	}, {
		// opaque, bit field. Not yet implemented.
		data:     []byte{15, 16, 2, 202, 254},
		expected: `opaque type 16 is not supported yet, with data [2 202 254]`,
	}}

	for _, tcase := range testcases {
		t.Run(tcase.expected, func(t *testing.T) {
			r, err := printJSONData(tcase.data)
			if err != nil {
				if got := err.Error(); !strings.HasPrefix(got, tcase.expected) {
					t.Errorf("unexpected output for %v: got [%v] expected [%v]", tcase.data, got, tcase.expected)
				}
			} else {
				if got := string(r); got != tcase.expected {
					t.Errorf("unexpected output for %v: got [%v] expected [%v]", tcase.data, got, tcase.expected)
				}
			}

		})
	}
}
