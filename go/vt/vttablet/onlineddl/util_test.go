package onlineddl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRandomHash(t *testing.T) {
	h1 := RandomHash()
	h2 := RandomHash()

	assert.Equal(t, len(h1), 64)
	assert.Equal(t, len(h2), 64)
	assert.NotEqual(t, h1, h2)
}

func TestToReadableTimestamp(t *testing.T) {
	ti, err := time.Parse(time.UnixDate, "Wed Feb 25 11:06:39 PST 2015")
	assert.NoError(t, err)

	readableTimestamp := ToReadableTimestamp(ti)
	assert.Equal(t, readableTimestamp, "20150225110639")
}
