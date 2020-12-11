package vtsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigParse(t *testing.T) {
	cfg := Config{}

	// This asserts we do not attempt to load a credentialsFlag via its Set func
	// if it's not specified in the args slice.
	err := cfg.Parse([]string{})
	assert.NoError(t, err)
}
