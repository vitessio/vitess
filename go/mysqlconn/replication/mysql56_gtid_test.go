package replication

import (
	"strings"
	"testing"
)

func TestParseMysql56GTID(t *testing.T) {
	input := "00010203-0405-0607-0809-0A0B0C0D0E0F:56789"
	want := Mysql56GTID{
		Server:   SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Sequence: 56789,
	}

	got, err := parseMysql56GTID(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("parseMysql56GTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestParseMysql56GTIDInvalid(t *testing.T) {
	table := []string{
		"",
		"00010203-0405-0607-0809-0A0B0C0D0E0F",
		"00010203-0405-0607-0809-0A0B0C0D0E0F:1-5",
		"00010203-0405-0607-0809-0A0B0C0D0E0F:1:2",
		"00010203-0405-0607-0809-0A0B0C0D0E0X:1",
	}

	for _, input := range table {
		_, err := parseMysql56GTID(input)
		if err == nil {
			t.Errorf("parseMysql56GTID(%#v): expected error, got none", input)
		}
	}
}

func TestSIDString(t *testing.T) {
	input := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	want := "00010203-0405-0607-0809-0a0b0c0d0e0f"

	if got := strings.ToLower(input.String()); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestParseSID(t *testing.T) {
	input := "00010203-0405-0607-0809-0A0B0C0D0E0F"
	want := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	got, err := ParseSID(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("ParseSID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestParseSIDInvalid(t *testing.T) {
	table := []string{
		"123",
		"x",
		"00010203-0405-0607-0809-0A0B0C0D0E0x",
		"00010203-0405-0607-080900A0B0C0D0E0F",
	}

	for _, input := range table {
		_, err := ParseSID(input)
		if err == nil {
			t.Errorf("ParseSID(%#v): expected error, got none", input)
		}
	}
}

func TestMysql56GTIDString(t *testing.T) {
	input := Mysql56GTID{
		Server:   SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Sequence: 12345,
	}
	want := "00010203-0405-0607-0809-0a0b0c0d0e0f:12345"
	if got := strings.ToLower(input.String()); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestMysql56GTIDFlavor(t *testing.T) {
	input := Mysql56GTID{}
	if got, want := input.Flavor(), "MySQL56"; got != want {
		t.Errorf("%#v.Flavor() = %#v, want %#v", input, got, want)
	}
}

func TestMysql56SequenceDomain(t *testing.T) {
	input := Mysql56GTID{}
	if got, want := input.SequenceDomain(), interface{}(nil); got != want {
		t.Errorf("%#v.SequenceDomain() = %#v, want %#v", input, got, want)
	}
}

func TestMysql56SourceServer(t *testing.T) {
	input := Mysql56GTID{
		Server: SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	}
	want := interface{}(SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	if got := input.SourceServer(); got != want {
		t.Errorf("%#v.SourceServer() = %#v, want %#v", input, got, want)
	}
}

func TestMysql56SequenceNumber(t *testing.T) {
	input := Mysql56GTID{
		Server:   SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Sequence: 5432,
	}
	want := interface{}(int64(5432))
	if got := input.SequenceNumber(); got != want {
		t.Errorf("%#v.SequenceNumber() = %#v, want %#v", input, got, want)
	}
}

func TestMysql56GTIDGTIDSet(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	input := Mysql56GTID{Server: sid1, Sequence: 5432}
	want := Mysql56GTIDSet{sid1: []interval{{5432, 5432}}}
	if got := input.GTIDSet(); !got.Equal(want) {
		t.Errorf("%#v.GTIDSet() = %#v, want %#v", input, got, want)
	}
}
