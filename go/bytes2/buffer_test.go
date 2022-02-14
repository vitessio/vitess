package bytes2

import (
	"testing"
)

func TestBuffer(t *testing.T) {
	b := NewBuffer(nil)
	b.Write([]byte("ab"))
	b.WriteString("cd")
	b.WriteByte('e')
	want := "abcde"
	if got := string(b.Bytes()); got != want {
		t.Errorf("b.Bytes(): %s, want %s", got, want)
	}
	if got := b.String(); got != want {
		t.Errorf("b.String(): %s, want %s", got, want)
	}
	if got := b.Len(); got != 5 {
		t.Errorf("b.Len(): %d, want 5", got)
	}
}
