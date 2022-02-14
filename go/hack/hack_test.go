package hack

import (
	"testing"
)

func TestByteToString(t *testing.T) {
	v1 := []byte("1234")
	if s := String(v1); s != "1234" {
		t.Errorf("String(\"1234\"): %q, want 1234", s)
	}

	v1 = []byte("")
	if s := String(v1); s != "" {
		t.Errorf("String(\"\"): %q, want empty", s)
	}

	v1 = nil
	if s := String(v1); s != "" {
		t.Errorf("String(\"\"): %q, want empty", s)
	}
}
