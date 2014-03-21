// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bson

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/bytes2"
)

type String1 string

func (cs String1) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	// Hardcode value to verify that function is called
	EncodeString(buf, key, "test")
}

type String2 string

func (cs *String2) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	// Hardcode value to verify that function is called
	EncodeString(buf, key, "test")
}

var marshaltest = []struct {
	desc string
	in   interface{}
	out  string
}{{
	"struct encode",
	struct{ Val string }{"test"},
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
}, {
	"struct encode nil",
	struct{ Val *int }{},
	"\n\x00\x00\x00\nVal\x00\x00",
}, {
	"struct encode nil interface",
	struct{ Val interface{} }{},
	"\n\x00\x00\x00\nVal\x00\x00",
}, {
	"map encode",
	map[string]string{"Val": "test"},
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
}, {
	"embedded map encode",
	struct{ Inner map[string]string }{map[string]string{"Val": "test"}},
	"\x1f\x00\x00\x00\x03Inner\x00\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00\x00",
}, {
	"embedded map encode nil",
	struct{ Inner map[string]string }{},
	"\f\x00\x00\x00\nInner\x00\x00",
}, {
	"slice encode",
	[]string{"test1", "test2"},
	"\x1f\x00\x00\x00\x050\x00\x05\x00\x00\x00\x00test1\x051\x00\x05\x00\x00\x00\x00test2\x00",
}, {
	"embedded slice encode",
	struct{ Inner []string }{[]string{"test1", "test2"}},
	"+\x00\x00\x00\x04Inner\x00\x1f\x00\x00\x00\x050\x00\x05\x00\x00\x00\x00test1\x051\x00\x05\x00\x00\x00\x00test2\x00\x00",
}, {
	"embedded slice encode nil",
	struct{ Inner []string }{},
	"\f\x00\x00\x00\nInner\x00\x00",
}, {
	"array encode",
	[2]string{"test1", "test2"},
	"\x1f\x00\x00\x00\x050\x00\x05\x00\x00\x00\x00test1\x051\x00\x05\x00\x00\x00\x00test2\x00",
}, {
	"string encode",
	"test",
	"\x15\x00\x00\x00\x05_Val_\x00\x04\x00\x00\x00\x00test\x00",
}, {
	"bytes encode",
	[]byte("test"),
	"\x15\x00\x00\x00\x05_Val_\x00\x04\x00\x00\x00\x00test\x00",
}, {
	"int64 encode",
	int64(1),
	"\x14\x00\x00\x00\x12_Val_\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
}, {
	"int32 encode",
	int32(1),
	"\x10\x00\x00\x00\x10_Val_\x00\x01\x00\x00\x00\x00",
}, {
	"int encode",
	int(1),
	"\x14\x00\x00\x00\x12_Val_\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
}, {
	"unit64 encode",
	uint64(1),
	"\x14\x00\x00\x00?_Val_\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
}, {
	"uint32 encode",
	uint32(1),
	"\x14\x00\x00\x00?_Val_\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
}, {
	"uint encode",
	uint(1),
	"\x14\x00\x00\x00?_Val_\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
}, {
	"float encode",
	float64(1.0),
	"\x14\x00\x00\x00\x01_Val_\x00\x00\x00\x00\x00\x00\x00\xf0?\x00",
}, {
	"bool encode",
	true,
	"\r\x00\x00\x00\b_Val_\x00\x01\x00",
}, {
	"time encode",
	time.Unix(1136243045, 0).UTC(),
	"\x14\x00\x00\x00\t_Val_\x00\x88\xf2\\\x8d\b\x01\x00\x00\x00",
}, {

	// Following encodes are for reference. They're used for
	// the decode tests.
	"embedded Object encode",
	struct{ Val struct{ Val2 string } }{struct{ Val2 string }{"test"}},
	"\x1e\x00\x00\x00\x03Val\x00\x14\x00\x00\x00\x05Val2\x00\x04\x00\x00\x00\x00test\x00\x00",
}, {
	"embedded Object encode nil element",
	struct{ Val struct{ Val2 *int64 } }{struct{ Val2 *int64 }{nil}},
	"\x15\x00\x00\x00\x03Val\x00\v\x00\x00\x00\nVal2\x00\x00\x00",
}, {
	"embedded Array encode",
	struct{ Val []string }{Val: []string{"test"}},
	"\x1b\x00\x00\x00\x04Val\x00\x11\x00\x00\x00\x050\x00\x04\x00\x00\x00\x00test\x00\x00",
}, {
	"Array encode nil element",
	struct{ Val []*int64 }{Val: []*int64{nil, newint64(1)}},
	"\x1d\x00\x00\x00\x04Val\x00\x13\x00\x00\x00\n0\x00\x121\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00",
}, {
	"embedded Number encode",
	struct{ Val float64 }{1.0},
	"\x12\x00\x00\x00\x01Val\x00\x00\x00\x00\x00\x00\x00\xf0?\x00",
}, {
	"embedded Binary encode",
	struct{ Val string }{"test"},
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
}, {
	"embedded Boolean encode",
	struct{ Val bool }{true},
	"\v\x00\x00\x00\bVal\x00\x01\x00",
}, {
	"embedded Datetime encode",
	struct{ Val time.Time }{time.Unix(1136243045, 0).UTC()},
	"\x12\x00\x00\x00\tVal\x00\x88\xf2\\\x8d\b\x01\x00\x00\x00",
}, {
	"embedded Null encode",
	struct{ Val *int }{},
	"\n\x00\x00\x00\nVal\x00\x00",
}, {
	"embedded Int encode",
	struct{ Val int32 }{1},
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
}, {
	"embedded Long encode",
	struct{ Val int64 }{1},
	"\x12\x00\x00\x00\x12Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
}, {
	"embedded Ulong encode",
	struct{ Val uint64 }{1},
	"\x12\x00\x00\x00?Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
}, {
	"embedded non-pointer encode with custom marshaler",
	struct{ Val String1 }{String1("foo")},
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
}, {
	"embedded pointer encode with custom marshaler",
	struct{ Val *String1 }{func(cs String1) *String1 { return &cs }(String1("foo"))},
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
}, {
	"embedded nil pointer encode with custom marshaler",
	struct{ Val *String1 }{},
	"\n\x00\x00\x00\nVal\x00\x00",
}, {
	"embedded pointer encode with custom pointer marshaler",
	struct{ Val *String2 }{func(cs String2) *String2 { return &cs }(String2("foo"))},
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
}, {
	"embedded addressable encode with custom pointer marshaler",
	&struct{ Val String2 }{String2("foo")},
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
}, {
	"embedded non-addressable encode with custom pointer marshaler",
	struct{ Val String2 }{String2("foo")},
	"\x12\x00\x00\x00\x05Val\x00\x03\x00\x00\x00\x00foo\x00",
}}

func TestMarshal(t *testing.T) {
	for _, tcase := range marshaltest {
		got := verifyMarshal(t, tcase.in)
		if string(got) != tcase.out {
			t.Errorf("%s: encoded: \n%q, want\n%q", tcase.desc, got, tcase.out)
		}
	}
}

var marshalErrorCases = []struct {
	desc string
	in   interface{}
	out  string
}{{
	"nil input",
	nil,
	"cannot marshal nil",
}, {
	"chan input",
	make(chan int),
	"unexpected type chan int",
}, {
	"embedded chan input",
	struct{ Val chan int }{},
	"don't know how to marshal chan int",
}, {
	"map with int key",
	map[int]int{},
	"can't marshall maps with non-string key types",
}}

func TestMarshalErrors(t *testing.T) {
	for _, tcase := range marshalErrorCases {
		_, err := Marshal(tcase.in)
		got := ""
		if err != nil {
			got = err.Error()
		}
		if got != tcase.out {
			t.Errorf("%s: received: %q, want %q", tcase.desc, got, tcase.out)
		}
	}
}
