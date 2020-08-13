/*
   Copyright 2014 Outbrain Inc.

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

package util

import (
	"reflect"
	"strings"
	"testing"

	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

func init() {
}

func TestTabulate(t *testing.T) {
	{
		text := strings.TrimSpace(`
a,b,c
d,e,f
g,h,i
	`)

		tabulated := Tabulate(strings.Split(text, "\n"), ",", ",")
		expected := strings.Split(text, "\n")
		test.S(t).ExpectTrue(reflect.DeepEqual(tabulated, expected))
	}
	{
		text := strings.TrimSpace(`
a,b,c
d,e,f
g,h,i
	`)

		tabulated := Tabulate(strings.Split(text, "\n"), ",", "|")
		expected := []string{
			"a|b|c",
			"d|e|f",
			"g|h|i",
		}
		test.S(t).ExpectTrue(reflect.DeepEqual(tabulated, expected))
	}
	{
		text := strings.TrimSpace(`
a,20,c
d,e,100
0000,h,i
	`)

		tabulated := Tabulate(strings.Split(text, "\n"), ",", "|")
		expected := []string{
			"a   |20|c  ",
			"d   |e |100",
			"0000|h |i  ",
		}
		test.S(t).ExpectTrue(reflect.DeepEqual(tabulated, expected))
	}
	{
		text := strings.TrimSpace(`
a,20,c
d,1,100
0000,3,i
	`)

		tabulated := Tabulate(strings.Split(text, "\n"), ",", "|", TabulateLeft, TabulateRight, TabulateRight)
		expected := []string{
			"a   |20|  c",
			"d   | 1|100",
			"0000| 3|  i",
		}

		test.S(t).ExpectTrue(reflect.DeepEqual(tabulated, expected))
	}
}
