/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRawInstanceKey(t *testing.T) {
	{
		key, err := newRawInstanceKey("127.0.0.1:3307")
		assert.NoError(t, err)
		assert.Equal(t, key.Hostname, "127.0.0.1")
		assert.Equal(t, key.Port, 3307)
	}
	{
		_, err := newRawInstanceKey("127.0.0.1:abcd")
		assert.Error(t, err)
	}
	{
		_, err := newRawInstanceKey("127.0.0.1:")
		assert.Error(t, err)
	}
	{
		_, err := newRawInstanceKey("127.0.0.1")
		assert.Error(t, err)
	}
}

func TestParseInstanceKey(t *testing.T) {
	{
		key, err := ParseInstanceKey("127.0.0.1:3307", 3306)
		assert.NoError(t, err)
		assert.Equal(t, "127.0.0.1", key.Hostname)
		assert.Equal(t, 3307, key.Port)
	}
	{
		key, err := ParseInstanceKey("127.0.0.1", 3306)
		assert.NoError(t, err)
		assert.Equal(t, "127.0.0.1", key.Hostname)
		assert.Equal(t, 3306, key.Port)
	}
}

func TestEquals(t *testing.T) {
	{
		expect := &InstanceKey{Hostname: "127.0.0.1", Port: 3306}
		key, err := ParseInstanceKey("127.0.0.1", 3306)
		assert.NoError(t, err)
		assert.True(t, key.Equals(expect))
	}
}

func TestStringCode(t *testing.T) {
	{
		key := &InstanceKey{Hostname: "127.0.0.1", Port: 3306}
		stringCode := key.StringCode()
		assert.Equal(t, "127.0.0.1:3306", stringCode)
	}
}
