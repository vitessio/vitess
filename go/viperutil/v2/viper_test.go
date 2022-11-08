package viperutil

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestGetFuncForType(t *testing.T) {
	v := viper.New()
	v.Set("foo", true)
	v.Set("bar", 5)
	v.Set("baz", []int{1, 2, 3})

	getBool := GetFuncForType[bool]()
	assert.True(t, getBool(v)("foo"))

	assert.Equal(t, 5, GetFuncForType[int]()(v)("bar"))
}
