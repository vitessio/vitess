package replication

import (
	"strings"
	"testing"
)

func TestParseGTID(t *testing.T) {
	flavor := "fake flavor"
	gtidParsers[flavor] = func(s string) (GTID, error) {
		return fakeGTID{value: s}, nil
	}
	input := "12345"
	want := fakeGTID{value: "12345"}

	got, err := ParseGTID(flavor, input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("ParseGTID(%#v, %#v) = %#v, want %#v", flavor, input, got, want)
	}
}

func TestMustParseGTID(t *testing.T) {
	flavor := "fake flavor"
	gtidParsers[flavor] = func(s string) (GTID, error) {
		return fakeGTID{value: s}, nil
	}
	input := "12345"
	want := fakeGTID{value: "12345"}

	got := MustParseGTID(flavor, input)
	if got != want {
		t.Errorf("MustParseGTID(%#v, %#v) = %#v, want %#v", flavor, input, got, want)
	}
}

func TestMustParseGTIDError(t *testing.T) {
	defer func() {
		want := `parse error: unknown GTID flavor "unknown flavor !@$!@"`
		err := recover()
		if err == nil {
			t.Errorf("wrong error, got %#v, want %#v", err, want)
		}
		got, ok := err.(error)
		if !ok || !strings.HasPrefix(got.Error(), want) {
			t.Errorf("wrong error, got %#v, want %#v", got, want)
		}
	}()

	MustParseGTID("unknown flavor !@$!@", "yowzah")
}

func TestParseUnknownFlavor(t *testing.T) {
	want := `parse error: unknown GTID flavor "foobar8675309"`

	_, err := ParseGTID("foobar8675309", "foo")
	if !strings.HasPrefix(err.Error(), want) {
		t.Errorf("wrong error, got '%v', want '%v'", err, want)
	}
}

func TestEncodeGTID(t *testing.T) {
	input := fakeGTID{
		flavor: "myflav",
		value:  "1:2:3-4-5-6",
	}
	want := "myflav/1:2:3-4-5-6"

	if got := EncodeGTID(input); got != want {
		t.Errorf("EncodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodeGTID(t *testing.T) {
	gtidParsers["flavorflav"] = func(s string) (GTID, error) {
		return fakeGTID{value: s}, nil
	}
	input := "flavorflav/123-456:789"
	want := fakeGTID{value: "123-456:789"}

	got, err := DecodeGTID(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("DecodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestMustDecodeGTID(t *testing.T) {
	gtidParsers["flavorflav"] = func(s string) (GTID, error) {
		return fakeGTID{value: s}, nil
	}
	input := "flavorflav/123-456:789"
	want := fakeGTID{value: "123-456:789"}

	got := MustDecodeGTID(input)
	if got != want {
		t.Errorf("DecodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestMustDecodeGTIDError(t *testing.T) {
	defer func() {
		want := `parse error: unknown GTID flavor "unknown flavor !@$!@"`
		err := recover()
		if err == nil {
			t.Errorf("wrong error, got %#v, want %#v", err, want)
		}
		got, ok := err.(error)
		if !ok || !strings.HasPrefix(got.Error(), want) {
			t.Errorf("wrong error, got %#v, want %#v", got, want)
		}
	}()

	MustDecodeGTID("unknown flavor !@$!@/yowzah")
}

func TestEncodeNilGTID(t *testing.T) {
	input := GTID(nil)
	want := ""

	if got := EncodeGTID(input); got != want {
		t.Errorf("EncodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodeNilGTID(t *testing.T) {
	input := ""
	want := GTID(nil)

	got, err := DecodeGTID(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("DecodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodeNoFlavor(t *testing.T) {
	gtidParsers[""] = func(s string) (GTID, error) {
		return fakeGTID{value: s}, nil
	}
	input := "12345"
	want := fakeGTID{value: "12345"}

	got, err := DecodeGTID(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("DecodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestDecodeGTIDWithSeparator(t *testing.T) {
	gtidParsers["moobar"] = func(s string) (GTID, error) {
		return fakeGTID{value: s}, nil
	}
	input := "moobar/GTID containing / a slash"
	want := fakeGTID{value: "GTID containing / a slash"}

	got, err := DecodeGTID(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("DecodeGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

type fakeGTID struct {
	flavor, value string
}

func (f fakeGTID) String() string            { return f.value }
func (f fakeGTID) Flavor() string            { return f.flavor }
func (fakeGTID) SourceServer() interface{}   { return int(1) }
func (fakeGTID) SequenceNumber() interface{} { return int(1) }
func (fakeGTID) SequenceDomain() interface{} { return int(1) }
func (f fakeGTID) GTIDSet() GTIDSet          { return nil }

func (fakeGTID) ContainsGTID(GTID) bool { return false }
func (fakeGTID) Contains(GTIDSet) bool  { return false }
func (f fakeGTID) Equal(other GTIDSet) bool {
	otherFake, ok := other.(fakeGTID)
	if !ok {
		return false
	}
	return f == otherFake
}
func (fakeGTID) AddGTID(GTID) GTIDSet { return nil }
