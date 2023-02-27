package util

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

func init() {
}

func TestNewToken(t *testing.T) {
	token1 := NewToken()

	require.NotEqual(t, token1.Hash, "")
	require.Equal(t, len(token1.Hash), 64)
}

func TestNewTokenRandom(t *testing.T) {
	log.Infof("test")
	token1 := NewToken()
	token2 := NewToken()

	// The following test can fail once in a quadrazillion eons
	require.NotEqual(t, token1.Hash, token2.Hash)
}
