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
}
