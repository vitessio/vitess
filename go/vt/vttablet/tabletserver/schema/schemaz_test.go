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

package schema

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchamazHandler1(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/schemaz", nil)
	tables := initialSchema()
	schemazHandler(tables, resp, req)
	body, _ := ioutil.ReadAll(resp.Body)

	test01 := []string{
		`<td>test_table_01</td>`,
		`<td>pk: INT32<br></td>`,
		`<td>pk<br></td>`,
		`<td>none</td>`,
	}
	matched, err := regexp.Match(strings.Join(test01, `\s*`), body)
	require.NoError(t, err)
	assert.True(t, matched, "test01 not matched in :%s", body)

	seq := []string{
		`<td>seq</td>`,
		`<td>id: INT32<br>next_id: INT64<br>cache: INT64<br>increment: INT64<br></td>`,
		`<td>id<br></td>`,
		`<td>sequence</td>`,
		`<td>{{0 0} 0 0}&lt;nil&gt;&lt;nil&gt;</td>`,
	}
	matched, err = regexp.Match(strings.Join(seq, `\s*`), body)
	require.NoError(t, err)
	assert.True(t, matched, "seq not matched in :%s", body)
}
