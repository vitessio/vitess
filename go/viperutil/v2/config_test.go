package viperutil

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetConfigHandlingValue(t *testing.T) {
	v := viper.New()
	v.SetDefault("default", ExitOnConfigFileNotFound)
	v.SetConfigType("yaml")

	cfg := `
foo: 2
bar: "2" # not valid, defaults to "ignore" (0)
baz: error
duration: 10h
`
	err := v.ReadConfig(strings.NewReader(strings.NewReplacer("\t", "  ").Replace(cfg)))
	require.NoError(t, err)

	getHandlingValueFunc := getHandlingValue(v)
	assert.Equal(t, ErrorOnConfigFileNotFound, getHandlingValueFunc("foo"), "failed to get int value")
	assert.Equal(t, IgnoreConfigFileNotFound, getHandlingValueFunc("bar"), "failed to get int-like string value")
	assert.Equal(t, ErrorOnConfigFileNotFound, getHandlingValueFunc("baz"), "failed to get string value")
	assert.Equal(t, IgnoreConfigFileNotFound, getHandlingValueFunc("notset"), "failed to get value on unset key")
	assert.Equal(t, IgnoreConfigFileNotFound, getHandlingValueFunc("duration"), "failed to get value on duration key")
	assert.Equal(t, ExitOnConfigFileNotFound, getHandlingValueFunc("default"), "failed to get value on default key")
}
