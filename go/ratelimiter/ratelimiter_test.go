package ratelimiter

import (
	"testing"
	"time"
)

func TestLimiter1(t *testing.T) {
	rl := NewRateLimiter(1, 10*time.Millisecond)
	result := rl.Allow()
	if !result {
		t.Error("Allow: false, want true")
	}
	result = rl.Allow()
	if result {
		t.Error("Allow: true, want false")
	}

	time.Sleep(11 * time.Millisecond)
	result = rl.Allow()
	if !result {
		t.Error("Allow: false, want true")
	}
	result = rl.Allow()
	if result {
		t.Error("Allow: true, want false")
	}
}

func TestLimiter2(t *testing.T) {
	rl := NewRateLimiter(2, 10*time.Millisecond)
	var result bool
	for i := 0; i < 2; i++ {
		result = rl.Allow()
		if !result {
			t.Errorf("Allow(%d): false, want true", i)
		}
	}
	result = rl.Allow()
	if result {
		t.Error("Allow: true, want false")
	}

	time.Sleep(11 * time.Millisecond)
	for i := 0; i < 2; i++ {
		result = rl.Allow()
		if !result {
			t.Errorf("Allow(%d): false, want true", i)
		}
	}
	result = rl.Allow()
	if result {
		t.Error("Allow: true, want false")
	}
}
