/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewProbe(t *testing.T) {
	c := NewProbe()
	assert.Equal(t, "", c.Key.Hostname)
	assert.Equal(t, 0, c.Key.Port)
	assert.Equal(t, "", c.User)
	assert.Equal(t, "", c.Password)
}

func TestDuplicateCredentials(t *testing.T) {
	c := NewProbe()
	c.Key = InstanceKey{Hostname: "myhost", Port: 3306}
	c.User = "gromit"
	c.Password = "penguin"

	dup := c.DuplicateCredentials(InstanceKey{Hostname: "otherhost", Port: 3310})
	assert.Equal(t, "otherhost", dup.Key.Hostname)
	assert.Equal(t, 3310, dup.Key.Port)
	assert.Equal(t, "gromit", dup.User)
	assert.Equal(t, "penguin", dup.Password)
}

func TestDuplicate(t *testing.T) {
	c := NewProbe()
	c.Key = InstanceKey{Hostname: "myhost", Port: 3306}
	c.User = "gromit"
	c.Password = "penguin"

	dup := c.Duplicate()
	assert.Equal(t, "myhost", dup.Key.Hostname)
	assert.Equal(t, 3306, dup.Key.Port)
	assert.Equal(t, "gromit", dup.User)
	assert.Equal(t, "penguin", dup.Password)
}
