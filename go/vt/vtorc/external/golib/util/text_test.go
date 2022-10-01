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

	"github.com/stretchr/testify/require"
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
		require.True(t, reflect.DeepEqual(tabulated, expected))
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
		require.True(t, reflect.DeepEqual(tabulated, expected))
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
		require.True(t, reflect.DeepEqual(tabulated, expected))
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

		require.True(t, reflect.DeepEqual(tabulated, expected))
	}
}
