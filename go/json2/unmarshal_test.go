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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestUnmarshal(t *testing.T) {
	tcases := []struct {
		in, err string
	}{{
		in: `{
  "l2": "val",
  "l3": [
    "l4",
    "l5"asdas"
  ]
}`,
		err: "line: 5, position 9: invalid character 'a' after array element",
	}, {
		in:  "{}",
		err: "",
	}}
	for _, tcase := range tcases {
		out := make(map[string]interface{})
		err := Unmarshal([]byte(tcase.in), &out)

		got := ""
		if err != nil {
			got = err.Error()
		}
		assert.Equal(t, tcase.err, got, "Unmarshal(%v) err", tcase.in)
	}
}

func TestUnmarshalProto(t *testing.T) {
	protoData := &emptypb.Empty{}
	protoJSONData, err := protojson.Marshal(protoData)
	assert.Nil(t, err, "protojson.Marshal error")

	tcase := struct {
		in  string
		out *emptypb.Empty
	}{
		in:  string(protoJSONData),
		out: &emptypb.Empty{},
	}

	err = Unmarshal([]byte(tcase.in), tcase.out)

	assert.Nil(t, err, "Unmarshal(%v) protobuf message", tcase.in)
	assert.Equal(t, protoData, tcase.out, "Unmarshal(%v) protobuf message result", tcase.in)
}

func TestAnnotate(t *testing.T) {
	tcases := []struct {
		data []byte
		err  error
	}{
		{
			data: []byte("invalid JSON"),
			err:  fmt.Errorf("line: 1, position 1: invalid character 'i' looking for beginning of value"),
		},
	}

	for _, tcase := range tcases {
		err := annotate(tcase.data, tcase.err)

		require.Equal(t, tcase.err, err, "annotate(%s, %v) error", string(tcase.data), tcase.err)
	}
}

func TestUnmarshalPB(t *testing.T) {
	want := &emptypb.Empty{}
	json, err := protojson.Marshal(want)
	require.NoError(t, err)

	var got emptypb.Empty
	err = UnmarshalPB(json, &got)
	require.NoError(t, err)
	require.Equal(t, want, &got)
}
