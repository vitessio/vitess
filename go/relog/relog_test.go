package relog

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

type sensitive struct {
	Password string
	Normal   string
}

func (s sensitive) Redacted() interface{} {
	s.Password = Redact(s.Password)
	return s
}

func TestRedacted(t *testing.T) {
	s := sensitive{"dupa55", "normal"}
	var _ Redactor = s
	for _, format := range []string{"%s", "%v", "%#v", "%q"} {
		b := new(bytes.Buffer)
		log := New(b, "test", log.LstdFlags, DEBUG)
		log.Info(format, s)

		if logged := b.String(); strings.Contains(logged, s.Password) {
			t.Errorf("Not redacted: %#v in %#v.", s.Password, logged)
		}
	}
}
