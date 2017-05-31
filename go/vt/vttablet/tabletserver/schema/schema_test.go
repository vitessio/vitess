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

package schema

import (
	"fmt"
	"testing"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

func TestTableColumnString(t *testing.T) {
	c := &TableColumn{Name: sqlparser.NewColIdent("my_column"), Type: querypb.Type_INT8}
	want := "{Name: 'my_column', Type: INT8}"
	got := fmt.Sprintf("%v", c)
	if got != want {
		t.Errorf("want: %v, got: %v", want, got)
	}
}
