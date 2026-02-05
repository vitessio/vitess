/*
Copyright 2024 The Vitess Authors.
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

package yaml2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestYamlVars(t *testing.T) {
	type TestStruct struct {
		StringField  string  `yaml:"stringfield"`
		IntField     int     `yaml:"intfield"`
		BoolField    bool    `yaml:"boolfield"`
		Float64Field float64 `yaml:"float64field"`
	}

	inputData := TestStruct{
		"tricky text to test text",
		32,
		true,
		3.141,
	}

	//testing Marshal
	var marshalData []byte
	var err error
	t.Run("Marshal", func(t *testing.T) {
		marshalData, err = Marshal(inputData)
		assert.NoError(t, err)
		require.EqualValues(t, `BoolField: true
Float64Field: 3.141
IntField: 32
StringField: tricky text to test text
`, string(marshalData))
	})

	//testing Unmarshal
	t.Run("Unmarshal", func(t *testing.T) {
		var unmarshalData TestStruct
		err = Unmarshal(marshalData, &unmarshalData)
		assert.NoError(t, err)
		assert.Equal(t, inputData, unmarshalData)

		unmarshalData.StringField = "changed text"
		assert.NotEqual(t, inputData, unmarshalData)
	})
}
