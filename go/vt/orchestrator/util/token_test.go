package util

import (
	"testing"

	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

func init() {
}

func TestNewToken(t *testing.T) {
	token1 := NewToken()

	test.S(t).ExpectNotEquals(token1.Hash, "")
	test.S(t).ExpectEquals(len(token1.Hash), 64)
}

func TestNewTokenRandom(t *testing.T) {
	token1 := NewToken()
	token2 := NewToken()

	// The following test can fail once in a quadrazillion eons
	test.S(t).ExpectNotEquals(token1.Hash, token2.Hash)
}
