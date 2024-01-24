package yaml2

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
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
	marshaledData, err := Marshal(inputData)
	assert.NoError(t, err)

	//testing Unmarshal
	var unmarshaledData TestStruct
	err = Unmarshal(marshaledData, &unmarshaledData)
	assert.NoError(t, err)
	if !reflect.DeepEqual(inputData, unmarshaledData) {
		t.Errorf("Unmarshaled data does not match original data. Expected: %v, Got: %v", inputData, unmarshaledData)
	}

}
