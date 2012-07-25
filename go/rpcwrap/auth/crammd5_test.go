package auth

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestCRAMMD5GetChallenge(t *testing.T) {
	hostname, _ := os.Hostname()
	challengePattern := fmt.Sprintf("<\\d+\\.\\d+@%s>", hostname)
	challenge, _ := CRAMMD5GetChallenge()
	if m, _ := regexp.MatchString(challengePattern, challenge); !m {
		t.Errorf("The challenge %s doesn't look like an RFC822 msg-id (%s).", challengePattern, challenge)
	}
}

func TestCRAMMD5GetExpected(t *testing.T) {
	expected := CRAMMD5GetExpected("ala", "ma kota", "<3629790421.390934@spinoza22.mtv.corp.google.com>")

	if !strings.HasPrefix(expected, "ala ") {
		t.Errorf("The proof doesn't start with the username: %s.", expected)
	}

	if length := len(expected); length != 32+len("ala ") {
		t.Errorf("%s doesn't look like a correct CRAM-MD5 digest (the length is not 32).", expected)
	}
}
