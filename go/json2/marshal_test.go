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

package json2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func TestMarshalPB(t *testing.T) {
	col := &vschemapb.Column{
		Name: "c1",
		Type: querypb.Type_VARCHAR,
	}
	b, err := MarshalPB(col)

	require.NoError(t, err, "MarshalPB(col) error")
	want := "{\"name\":\"c1\",\"type\":\"VARCHAR\"}"
	assert.Equal(t, want, string(b), "MarshalPB(col)")
}

func TestMarshalIndentPB(t *testing.T) {
	col := &vschemapb.Column{
		Name: "c1",
		Type: querypb.Type_VARCHAR,
	}
	indent := "  "
	b, err := MarshalIndentPB(col, indent)

	require.NoError(t, err, "MarshalIndentPB(col, indent) error")
	want := "{\n  \"name\": \"c1\",\n  \"type\": \"VARCHAR\"\n}"
	assert.Equal(t, want, string(b), "MarshalIndentPB(col, indent)")
}
