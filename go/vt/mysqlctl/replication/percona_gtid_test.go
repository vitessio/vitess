// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package replication

import (
	"strings"
	"testing"
)

func TestParsePerconaGTID(t *testing.T) {
	input := "00010203-0405-0607-0809-0A0B0C0D0E0F:56789"
	want := PerconaGTID{
		Server:   PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Sequence: 56789,
	}

	got, err := parsePerconaGTID(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("parsePerconaGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestParsePerconaGTIDInvalid(t *testing.T) {
	table := []string{
		"",
		"00010203-0405-0607-0809-0A0B0C0D0E0F",
		"00010203-0405-0607-0809-0A0B0C0D0E0F:1-5",
		"00010203-0405-0607-0809-0A0B0C0D0E0F:1:2",
		"00010203-0405-0607-0809-0A0B0C0D0E0X:1",
	}

	for _, input := range table {
		_, err := parsePerconaGTID(input)
		if err == nil {
			t.Errorf("parsePerconaGTID(%#v): expected error, got none", input)
		}
	}
}

func TestPSIDString(t *testing.T) {
	input := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	want := "00010203-0405-0607-0809-0a0b0c0d0e0f"

	if got := strings.ToLower(input.String()); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestParsePSID(t *testing.T) {
	input := "00010203-0405-0607-0809-0A0B0C0D0E0F"
	want := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	got, err := ParsePSID(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("ParsePSID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestParsePSIDInvalid(t *testing.T) {
	table := []string{
		"123",
		"x",
		"00010203-0405-0607-0809-0A0B0C0D0E0x",
		"00010203-0405-0607-080900A0B0C0D0E0F",
	}

	for _, input := range table {
		_, err := ParsePSID(input)
		if err == nil {
			t.Errorf("ParsePSID(%#v): expected error, got none", input)
		}
	}
}

func TestPerconaGTIDString(t *testing.T) {
	input := PerconaGTID{
		Server:   PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Sequence: 12345,
	}
	want := "00010203-0405-0607-0809-0a0b0c0d0e0f:12345"
	if got := strings.ToLower(input.String()); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestPerconaGTIDFlavor(t *testing.T) {
	input := PerconaGTID{}
	if got, want := input.Flavor(), "Percona"; got != want {
		t.Errorf("%#v.Flavor() = %#v, want %#v", input, got, want)
	}
}

func TestPerconaSequenceDomain(t *testing.T) {
	input := PerconaGTID{}
	if got, want := input.SequenceDomain(), interface{}(nil); got != want {
		t.Errorf("%#v.SequenceDomain() = %#v, want %#v", input, got, want)
	}
}

func TestPerconaSourceServer(t *testing.T) {
	input := PerconaGTID{
		Server: PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	}
	want := interface{}(PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	if got := input.SourceServer(); got != want {
		t.Errorf("%#v.SourceServer() = %#v, want %#v", input, got, want)
	}
}

func TestPerconaSequenceNumber(t *testing.T) {
	input := PerconaGTID{
		Server:   PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Sequence: 5432,
	}
	want := interface{}(int64(5432))
	if got := input.SequenceNumber(); got != want {
		t.Errorf("%#v.SequenceNumber() = %#v, want %#v", input, got, want)
	}
}

func TestPerconaGTIDGTIDSet(t *testing.T) {
	sid1 := PSID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	input := PerconaGTID{Server: sid1, Sequence: 5432}
	want := PerconaGTIDSet{sid1: []interval{{5432, 5432}}}
	if got := input.GTIDSet(); !got.Equal(want) {
		t.Errorf("%#v.GTIDSet() = %#v, want %#v", input, got, want)
	}
}
