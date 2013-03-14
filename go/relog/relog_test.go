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
		log := New(b, "test", DEBUG)
		log.Info(format, s)

		if logged := b.String(); strings.Contains(logged, s.Password) {
			t.Errorf("Not redacted: %#v in %#v.", s.Password, logged)
		}
	}
}

func checkLogOutput(t *testing.T, line string) {
	if n := strings.Count(line, "\n"); n != 1 {
		t.Errorf("only one newline allowed: %v in %#v.", n, line)
	}
	if n := strings.Count(line, "\t"); n != 5 {
		t.Errorf("wrong number of fields (bad tab escaping?): %v in %#v.", n, line)
	}
}

func TestOutputWithData(t *testing.T) {
	b := new(bytes.Buffer)
	l := New(b, "test", DEBUG)
	l.SetFlags(Ltsv | Lblob)
	data := map[string]interface{}{"x": 1, "y\nz": "some\nmultiline\ndata"}
	l.OutputWithData(INFO, data, "test default info logging")
	checkLogOutput(t, b.String())
}

func TestLogWithTabs(t *testing.T) {
	b := new(bytes.Buffer)
	l := New(b, "test", DEBUG)
	l.SetFlags(Ltsv | Lblob)
	HijackLog(l)
	log.Print("shim api message with tabs\t\t\n")
	checkLogOutput(t, b.String())
}

func TestLogWithNewlines(t *testing.T) {
	b := new(bytes.Buffer)
	l := New(b, "test", DEBUG)
	l.SetFlags(Ltsv | Lblob)
	HijackLog(l)
	log.Print("shim api message with newlines\n\n\n")
	checkLogOutput(t, b.String())
}

func TestLog(t *testing.T) {
	b := new(bytes.Buffer)
	l := New(b, "test", DEBUG)
	l.SetFlags(Ltsv | Lblob)
	HijackLog(l)
	log.Print("shim api message")
	checkLogOutput(t, b.String())
}
