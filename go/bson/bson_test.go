// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bson

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/bytes2"
)

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
}}

func TestMarshal(t *testing.T) {
	for _, tcase := range marshaltest {
		got := verifyMarshal(t, tcase.in)
		if string(got) != tcase.out {
			t.Errorf("%s: encoded: \n%q, want\n%q", tcase.desc, got, tcase.out)
		}
	}
}

func newstring(v string) *string              { return &v }
func newint64(v int64) *int64                 { return &v }
func newint32(v int32) *int32                 { return &v }
func newint(v int) *int                       { return &v }
func newuint64(v uint64) *uint64              { return &v }
func newuint32(v uint32) *uint32              { return &v }
func newuint(v uint) *uint                    { return &v }
func newfloat64(v float64) *float64           { return &v }
func newbool(v bool) *bool                    { return &v }
func newtime(v time.Time) *time.Time          { return &v }
func newinterface(v interface{}) *interface{} { return &v }

var unmarshaltest = []struct {
	desc string
	in   string
	out  interface{}
	want interface{}
}{{

	// top level decodes
	"top level nil decode",
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
	nil,
	nil,
}, {
	"top level struct decode",
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
	&struct{ Val string }{},
	&struct{ Val string }{"test"},
}, {
	"top level map decode",
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
	&map[string]string{},
	&map[string]string{"Val": "test"},
}, {
	"top level slice decode",
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
	&[]string{},
	&[]string{"test"},
}, {
	"top level array decode",
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
	&[2]string{},
	&[2]string{"test", ""},
}, {
	"top level string decode",
	"\x15\x00\x00\x00\x05_Val_\x00\x04\x00\x00\x00\x00test\x00",
	newstring(""),
	newstring("test"),
}, {
	"top level string decode from Null",
	"\x0c\x00\x00\x00\n_Val_\x00\x00",
	newstring("test"),
	newstring(""),
}, {
	"top level bytes decode",
	"\x15\x00\x00\x00\x05_Val_\x00\x04\x00\x00\x00\x00test\x00",
	&[]byte{},
	&[]byte{'t', 'e', 's', 't'},
}, {
	"top level int64 decode",
	"\x14\x00\x00\x00\x12_Val_\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	newint64(0),
	newint64(1),
}, {
	"top level int32 decode",
	"\x10\x00\x00\x00\x10_Val_\x00\x01\x00\x00\x00\x00",
	newint32(0),
	newint32(1),
}, {
	"top level int decode",
	"\x10\x00\x00\x00\x10_Val_\x00\x01\x00\x00\x00\x00",
	newint(0),
	newint(1),
}, {
	"top level uint64 decode",
	"\x14\x00\x00\x00?_Val_\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	newuint64(0),
	newuint64(1),
}, {
	"top level uint32 decode",
	"\x14\x00\x00\x00?_Val_\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	newuint32(0),
	newuint32(1),
}, {
	"top level uint decode",
	"\x14\x00\x00\x00\x12_Val_\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	newuint(0),
	newuint(1),
}, {
	"top level float64 decode",
	"\x14\x00\x00\x00\x01_Val_\x00\x00\x00\x00\x00\x00\x00\xf0?\x00",
	newfloat64(0),
	newfloat64(1.0),
}, {
	"top level bool decode",
	"\r\x00\x00\x00\b_Val_\x00\x01\x00",
	newbool(false),
	newbool(true),
}, {
	"top level time decode",
	"\x14\x00\x00\x00\t_Val_\x00\x88\xf2\\\x8d\b\x01\x00\x00\x00",
	newtime(time.Now()),
	newtime(time.Unix(1136243045, 0).UTC()),
}, {
	"top level interface decode",
	"\x14\x00\x00\x00\x01_Val_\x00\x00\x00\x00\x00\x00\x00\xf0?\x00",
	newinterface(nil),
	newinterface(float64(1.0)),
}, {

	// embedded decodes
	"struct decode from Object",
	"\x1e\x00\x00\x00\x03Val\x00\x14\x00\x00\x00\x05Val2\x00\x04\x00\x00\x00\x00test\x00\x00",
	&struct{ Val struct{ Val2 string } }{},
	&struct{ Val struct{ Val2 string } }{struct{ Val2 string }{"test"}},
}, {
	"struct decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val struct{ Val2 string } }{struct{ Val2 string }{"test"}},
	&struct{ Val struct{ Val2 string } }{},
}, {
	"map decode from Object",
	"\x1e\x00\x00\x00\x03Val\x00\x14\x00\x00\x00\x05Val2\x00\x04\x00\x00\x00\x00test\x00\x00",
	&struct{ Val map[string]string }{},
	&struct{ Val map[string]string }{map[string]string{"Val2": "test"}},
}, {
	"map decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val map[string]string }{map[string]string{"Val2": "test"}},
	&struct{ Val map[string]string }{},
}, {
	"map decode from Null element",
	"\x15\x00\x00\x00\x03Val\x00\v\x00\x00\x00\nVal2\x00\x00\x00",
	&struct{ Val map[string]string }{},
	&struct{ Val map[string]string }{map[string]string{"Val2": ""}},
}, {
	"slice decode from Array",
	"\x1b\x00\x00\x00\x04Val\x00\x11\x00\x00\x00\x050\x00\x04\x00\x00\x00\x00test\x00\x00",
	&struct{ Val []string }{},
	&struct{ Val []string }{[]string{"test"}},
}, {
	"slice decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val []string }{[]string{"test"}},
	&struct{ Val []string }{},
}, {
	"slice decode from Null element",
	"\x1d\x00\x00\x00\x04Val\x00\x13\x00\x00\x00\n0\x00\x121\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val []*int64 }{},
	&struct{ Val []*int64 }{[]*int64{nil, newint64(1)}},
}, {
	"array decode from Array",
	"\x1b\x00\x00\x00\x04Val\x00\x11\x00\x00\x00\x050\x00\x04\x00\x00\x00\x00test\x00\x00",
	&struct{ Val [2]string }{},
	&struct{ Val [2]string }{[2]string{"test", ""}},
}, {
	"array decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val [2]string }{[2]string{"test", ""}},
	&struct{ Val [2]string }{},
}, {
	"array decode from Null element",
	"\x1d\x00\x00\x00\x04Val\x00\x13\x00\x00\x00\n0\x00\x121\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val [2]*int64 }{},
	&struct{ Val [2]*int64 }{[2]*int64{nil, newint64(1)}},
}, {
	"string decode from String",
	"\x13\x00\x00\x00\x02Val\x00\x05\x00\x00\x00test\x00\x00",
	&struct{ Val string }{},
	&struct{ Val string }{"test"},
}, {
	"string decode from Binary",
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
	&struct{ Val string }{},
	&struct{ Val string }{"test"},
}, {
	"string decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val string }{"test"},
	&struct{ Val string }{},
}, {
	"bytes decode from String",
	"\x13\x00\x00\x00\x02Val\x00\x05\x00\x00\x00test\x00\x00",
	&struct{ Val []byte }{},
	&struct{ Val []byte }{[]byte("test")},
}, {
	"bytes decode from Binary",
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
	&struct{ Val []byte }{},
	&struct{ Val []byte }{[]byte("test")},
}, {
	"bytes decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val []byte }{[]byte("test")},
	&struct{ Val []byte }{},
}, {
	"int64 decode from Int",
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
	&struct{ Val int64 }{},
	&struct{ Val int64 }{1},
}, {
	"int64 decode from Long",
	"\x12\x00\x00\x00\x12Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val int64 }{},
	&struct{ Val int64 }{1},
}, {
	"int64 decode from Ulong",
	"\x12\x00\x00\x00?Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val int64 }{},
	&struct{ Val int64 }{1},
}, {
	"int64 decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val int64 }{1},
	&struct{ Val int64 }{},
}, {
	"int32 decode from Int",
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
	&struct{ Val int32 }{},
	&struct{ Val int32 }{1},
}, {
	"int32 decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val int32 }{1},
	&struct{ Val int32 }{},
}, {
	"int decode from Long",
	"\x12\x00\x00\x00\x12Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val int }{},
	&struct{ Val int }{1},
}, {
	"int decode from Int",
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
	&struct{ Val int }{},
	&struct{ Val int }{1},
}, {
	"int decode from Ulong",
	"\x12\x00\x00\x00?Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val int }{},
	&struct{ Val int }{1},
}, {
	"int decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val int }{1},
	&struct{ Val int }{},
}, {
	"uint64 decode from Int",
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
	&struct{ Val uint64 }{},
	&struct{ Val uint64 }{1},
}, {
	"uint64 decode from Long",
	"\x12\x00\x00\x00\x12Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val uint64 }{},
	&struct{ Val uint64 }{1},
}, {
	"uint64 decode from Ulong",
	"\x12\x00\x00\x00?Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val uint64 }{},
	&struct{ Val uint64 }{1},
}, {
	"uint64 decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val uint64 }{1},
	&struct{ Val uint64 }{},
}, {
	"uint32 decode from Int",
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
	&struct{ Val uint32 }{},
	&struct{ Val uint32 }{1},
}, {
	"uint32 decode from Ulong",
	"\x12\x00\x00\x00?Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val uint32 }{},
	&struct{ Val uint32 }{1},
}, {
	"uint32 decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val uint32 }{1},
	&struct{ Val uint32 }{},
}, {
	"uint decode from Int",
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
	&struct{ Val uint }{},
	&struct{ Val uint }{1},
}, {
	"uint decode from Long",
	"\x12\x00\x00\x00\x12Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val uint }{},
	&struct{ Val uint }{1},
}, {
	"uint decode from Ulong",
	"\x12\x00\x00\x00?Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val uint }{},
	&struct{ Val uint }{1},
}, {
	"uint decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val uint }{1},
	&struct{ Val uint }{},
}, {
	"float64 decode from Number",
	"\x12\x00\x00\x00\x01Val\x00\x00\x00\x00\x00\x00\x00\xf0?\x00",
	&struct{ Val float64 }{},
	&struct{ Val float64 }{1.0},
}, {
	"float64 decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val float64 }{1.0},
	&struct{ Val float64 }{},
}, {
	"bool decode from Boolean",
	"\v\x00\x00\x00\bVal\x00\x01\x00",
	&struct{ Val bool }{},
	&struct{ Val bool }{true},
}, {
	"bool decode from Int",
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
	&struct{ Val bool }{},
	&struct{ Val bool }{true},
}, {
	"bool decode from Long",
	"\x12\x00\x00\x00\x12Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val bool }{},
	&struct{ Val bool }{true},
}, {
	"bool decode from Ulong",
	"\x12\x00\x00\x00?Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val bool }{},
	&struct{ Val bool }{true},
}, {
	"bool decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val bool }{true},
	&struct{ Val bool }{},
}, {
	"time decode from Datetime",
	"\x12\x00\x00\x00\tVal\x00\x88\xf2\\\x8d\b\x01\x00\x00\x00",
	&struct{ Val time.Time }{},
	&struct{ Val time.Time }{time.Unix(1136243045, 0).UTC()},
}, {
	"time decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val time.Time }{time.Unix(1136243045, 0).UTC()},
	&struct{ Val time.Time }{},
}, {
	"interface decode from Number",
	"\x12\x00\x00\x00\x01Val\x00\x00\x00\x00\x00\x00\x00\xf0?\x00",
	&struct{ Val interface{} }{},
	&struct{ Val interface{} }{float64(1.0)},
}, {
	"interface decode from String",
	"\x13\x00\x00\x00\x02Val\x00\x05\x00\x00\x00test\x00\x00",
	&struct{ Val interface{} }{},
	&struct{ Val interface{} }{"test"},
}, {
	"interface decode from Binary",
	"\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00",
	&struct{ Val interface{} }{},
	&struct{ Val interface{} }{[]byte("test")},
}, {
	"interface decode from Boolean",
	"\v\x00\x00\x00\bVal\x00\x01\x00",
	&struct{ Val interface{} }{},
	&struct{ Val interface{} }{true},
}, {
	"interface decode from Datetime",
	"\x12\x00\x00\x00\tVal\x00\x88\xf2\\\x8d\b\x01\x00\x00\x00",
	&struct{ Val interface{} }{},
	&struct{ Val interface{} }{time.Unix(1136243045, 0).UTC()},
}, {
	"interface decode from Int",
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
	&struct{ Val interface{} }{},
	&struct{ Val interface{} }{int32(1)},
}, {
	"interface decode from Long",
	"\x12\x00\x00\x00\x12Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val interface{} }{},
	&struct{ Val interface{} }{int64(1)},
}, {
	"interface decode from Ulong",
	"\x12\x00\x00\x00?Val\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val interface{} }{},
	&struct{ Val interface{} }{uint64(1)},
}, {
	"interface decode from Object",
	"\x1e\x00\x00\x00\x03Val\x00\x14\x00\x00\x00\x05Val2\x00\x04\x00\x00\x00\x00test\x00\x00",
	&struct{ Val interface{} }{},
	&struct{ Val interface{} }{map[string]interface{}{"Val2": []byte("test")}},
}, {
	"interface decode from Object with Null element",
	"\x15\x00\x00\x00\x03Val\x00\v\x00\x00\x00\nVal2\x00\x00\x00",
	&struct{ Val interface{} }{},
	&struct{ Val interface{} }{map[string]interface{}{"Val2": nil}},
}, {
	"interface decode from Array",
	"\x1b\x00\x00\x00\x04Val\x00\x11\x00\x00\x00\x050\x00\x04\x00\x00\x00\x00test\x00\x00",
	&struct{ Val interface{} }{},
	&struct{ Val interface{} }{[]interface{}{[]byte("test")}},
}, {
	"interface decode from Array null element",
	"\x1d\x00\x00\x00\x04Val\x00\x13\x00\x00\x00\n0\x00\x121\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00",
	&struct{ Val interface{} }{},
	&struct{ Val interface{} }{[]interface{}{nil, int64(1)}},
}, {
	"interface decode from Null",
	"\n\x00\x00\x00\nVal\x00\x00",
	&struct{ Val interface{} }{uint64(1)},
	&struct{ Val interface{} }{},
}, {
	"pointer decode from Int",
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
	&struct{ Val *int64 }{},
	&struct{ Val *int64 }{newint64(1)},
}}

func TestUnmarshal(t *testing.T) {
	for _, tcase := range unmarshaltest {
		verifyUnmarshal(t, []byte(tcase.in), tcase.out)
		if !reflect.DeepEqual(tcase.out, tcase.want) {
			out := reflect.ValueOf(tcase.out).Elem().Interface()
			want := reflect.ValueOf(tcase.want).Elem().Interface()
			t.Errorf("%s: decoded: \n%#v, want\n%#v", tcase.desc, out, want)
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

var unmarshalErrorCases = []struct {
	desc string
	in   string
	out  interface{}
	want string
}{{
	"non pointer input",
	"",
	10,
	"expecting pointer value, received int",
}, {
	"invalid bson kind",
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
	&struct{ Val struct{ Val2 int } }{},
	"unexpected kind: 16",
}, {
	"map with int key",
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
	&map[int]int{},
	"map index is not a string: Val",
}, {
	"small array",
	"\x1f\x00\x00\x00\x050\x00\x05\x00\x00\x00\x00test1\x051\x00\x05\x00\x00\x00\x00test2\x00",
	&[1]string{},
	"array index 1 out of bounds",
}, {
	"chan in struct",
	"\x0e\x00\x00\x00\x10Val\x00\x01\x00\x00\x00\x00",
	&struct{ Val chan int }{},
	"cannot unmarshal into chan",
}}

func TestUnmarshalErrors(t *testing.T) {
	for _, tcase := range unmarshalErrorCases {
		err := Unmarshal([]byte(tcase.in), tcase.out)
		got := ""
		if err != nil {
			got = err.Error()
		}
		if got != tcase.want {
			t.Errorf("%s: received: %q, want %q", tcase.desc, got, tcase.want)
		}
	}
}

var customUnmarshalCases = []struct {
	desc    string
	in      string
	kind    byte
	decoder func(buf *bytes.Buffer, kind byte) interface{}
	out     interface{}
}{{
	"String->string",
	"\x05\x00\x00\x00test\x00",
	String,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeString(buf, kind) },
	"test",
}, {
	"Binary->string",
	"\x04\x00\x00\x00\x00test",
	Binary,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeString(buf, kind) },
	"test",
}, {
	"Null->string",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeString(buf, kind) },
	"",
}, {
	"String->bytes",
	"\x05\x00\x00\x00test\x00",
	String,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBinary(buf, kind) },
	[]byte("test"),
}, {
	"Binary->bytes",
	"\x04\x00\x00\x00\x00test",
	Binary,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBinary(buf, kind) },
	[]byte("test"),
}, {
	"Null->bytes",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBinary(buf, kind) },
	[]byte(nil),
}, {
	"Int->int64",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt64(buf, kind) },
	int64(1),
}, {
	"Long->int64",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Long,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt64(buf, kind) },
	int64(1),
}, {
	"Ulong->int64",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt64(buf, kind) },
	int64(1),
}, {
	"Null->int64",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt64(buf, kind) },
	int64(0),
}, {
	"Int->int32",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt32(buf, kind) },
	int32(1),
}, {
	"Null->int32",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt32(buf, kind) },
	int32(0),
}, {
	"Int->int",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt(buf, kind) },
	int(1),
}, {
	"Long->int",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Long,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt(buf, kind) },
	int(1),
}, {
	"Ulong->int",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt(buf, kind) },
	int(1),
}, {
	"Null->int",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt(buf, kind) },
	int(0),
}, {
	"Int->uint64",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint64(buf, kind) },
	uint64(1),
}, {
	"Long->uint64",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Long,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint64(buf, kind) },
	uint64(1),
}, {
	"Ulong->uint64",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint64(buf, kind) },
	uint64(1),
}, {
	"Null->uint64",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint64(buf, kind) },
	uint64(0),
}, {
	"Int->uint32",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint32(buf, kind) },
	uint32(1),
}, {
	"Ulong->uint32",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint32(buf, kind) },
	uint32(1),
}, {
	"Null->uint32",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint32(buf, kind) },
	uint32(0),
}, {
	"Int->uint",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint(buf, kind) },
	uint(1),
}, {
	"Long->uint",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Long,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint(buf, kind) },
	uint(1),
}, {
	"Ulong->uint",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint(buf, kind) },
	uint(1),
}, {
	"Null->uint",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint(buf, kind) },
	uint(0),
}, {
	"Number->float64",
	"\x00\x00\x00\x00\x00\x00\xf0?",
	Number,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeFloat64(buf, kind) },
	float64(1.0),
}, {
	"Null->float64",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeFloat64(buf, kind) },
	float64(0.0),
}, {
	"Boolean->bool",
	"\x01",
	Boolean,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBool(buf, kind) },
	true,
}, {
	"Null->bool",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBool(buf, kind) },
	false,
}, {
	"Datetime->time.Time",
	"\x88\xf2\\\x8d\b\x01\x00\x00",
	Datetime,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeTime(buf, kind) },
	time.Unix(1136243045, 0).UTC(),
}, {
	"Null->time.Time",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeTime(buf, kind) },
	time.Time{},
}, {
	"Number->interface{}",
	"\x00\x00\x00\x00\x00\x00\xf0?",
	Number,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	float64(1.0),
}, {
	"String->interface{}",
	"\x05\x00\x00\x00test\x00",
	String,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	"test",
}, {
	"Object->interface{}",
	"\x14\x00\x00\x00\x05Val2\x00\x04\x00\x00\x00\x00test\x00",
	Object,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	map[string]interface{}{"Val2": []byte("test")},
}, {
	"Object->interface{} with null element",
	"\v\x00\x00\x00\nVal2\x00\x00",
	Object,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	map[string]interface{}{"Val2": nil},
}, {
	"Array->interface{}",
	"\x11\x00\x00\x00\x050\x00\x04\x00\x00\x00\x00test\x00",
	Array,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	[]interface{}{[]byte("test")},
}, {
	"Array->interface{} with null element",
	"\x13\x00\x00\x00\n0\x00\x121\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	Array,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	[]interface{}{nil, int64(1)},
}, {
	"Binary->interface{}",
	"\x04\x00\x00\x00\x00test",
	Binary,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	[]byte("test"),
}, {
	"Boolean->interface{}",
	"\x01",
	Boolean,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	true,
}, {
	"Datetime->interface{}",
	"\x88\xf2\\\x8d\b\x01\x00\x00",
	Datetime,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	time.Unix(1136243045, 0).UTC(),
}, {
	"Int->interface{}",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	int32(1),
}, {
	"Long->interface{}",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Long,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	int64(1),
}, {
	"Ulong->interface{}",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	uint64(1),
}, {
	"Null->interface{}",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	nil,
}, {
	"Null->map[string]interface{}",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeMap(buf, kind) },
	map[string]interface{}(nil),
}, {
	"Null->[]interface{}",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeArray(buf, kind) },
	[]interface{}(nil),
}, {
	"Array->[]string",
	"\x1f\x00\x00\x00\x050\x00\x05\x00\x00\x00\x00test1\x051\x00\x05\x00\x00\x00\x00test2\x00",
	Array,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeStringArray(buf, kind) },
	[]string{"test1", "test2"},
}, {
	"Null->[]string",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeStringArray(buf, kind) },
	[]string(nil),
}}

func TestCustomUnmarshal(t *testing.T) {
	for _, tcase := range customUnmarshalCases {
		buf := bytes.NewBuffer([]byte(tcase.in))
		got := tcase.decoder(buf, tcase.kind)
		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("%s: received: %v, want %v", tcase.desc, got, tcase.out)
		}
		if buf.Len() != 0 {
			t.Errorf("%s: %d unread bytes from %q, want 0", tcase.desc, buf.Len(), tcase.in)
		}
	}
}

var customUnmarshalFailureCases = []struct {
	typ     string
	decoder func(buf *bytes.Buffer, kind byte) interface{}
	valid   []byte
}{{
	"string",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeString(buf, kind) },
	[]byte{String, Binary, Null},
}, {
	"[]byte",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBinary(buf, kind) },
	[]byte{String, Binary, Null},
}, {
	"int64",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt64(buf, kind) },
	[]byte{Int, Long, Ulong, Null},
}, {
	"int32",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt32(buf, kind) },
	[]byte{Int, Null},
}, {
	"int",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt(buf, kind) },
	[]byte{Int, Long, Ulong, Null},
}, {
	"uint64",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint64(buf, kind) },
	[]byte{Int, Long, Ulong, Null},
}, {
	"uint32",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint32(buf, kind) },
	[]byte{Int, Ulong, Null},
}, {
	"uint",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint(buf, kind) },
	[]byte{Int, Long, Ulong, Null},
}, {
	"float64",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeFloat64(buf, kind) },
	[]byte{Number, Null},
}, {
	"bool",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBool(buf, kind) },
	[]byte{Boolean, Int, Long, Ulong, Null},
}, {
	"time.Time",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeTime(buf, kind) },
	[]byte{Datetime, Null},
}, {
	"interface{}",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	[]byte{Number, String, Object, Array, Binary, Boolean, Datetime, Null, Int, Long, Ulong},
}, {
	"map",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeMap(buf, kind) },
	[]byte{Object, Null},
}, {
	"slice",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeArray(buf, kind) },
	[]byte{Array, Null},
}, {
	"[]string",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeStringArray(buf, kind) },
	[]byte{Array, Null},
}, {
	"skip",
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	[]byte{Number, String, Object, Array, Binary, Boolean, Datetime, Null, Int, Long, Ulong},
}}

func TestCustomUnmarshalFailures(t *testing.T) {
	allKinds := []byte{EOO, Number, String, Object, Array, Binary, Boolean, Datetime, Null, Int, Long, Ulong}
	for _, tcase := range customUnmarshalFailureCases {
		for _, kind := range allKinds {
			want := fmt.Sprintf("unexpected kind %v for %s", kind, tcase.typ)
			func() {
				defer func() {
					if x := recover(); x != nil {
						got := x.(BsonError).Error()
						if got != want {
							t.Errorf("got %s, want %s", got, want)
						}
						return
					}
				}()
				for _, valid := range tcase.valid {
					if kind == valid {
						return
					}
				}
				tcase.decoder(nil, kind)
				t.Errorf("got no error, want %s", want)
			}()
		}
	}
}

type alltypes struct {
	Bytes   []byte
	Float64 float64
	String  string
	Bool    bool
	Time    time.Time
	Int64   int64
	Int32   int32
	Int     int
	Uint64  uint64
	Uint32  uint32
	Uint    uint
	Map     map[string]int64
	Strings []string
	Nil     interface{}
}

func (a *alltypes) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	VerifyObject(kind)
	Next(buf, 4)

	kind = NextByte(buf)
	for kind != EOO {
		key := ReadCString(buf)
		switch key {
		case "Bytes":
			verifyKind("Bytes", Binary, kind)
			a.Bytes = DecodeBinary(buf, kind)
		case "Float64":
			verifyKind("Float64", Number, kind)
			a.Float64 = DecodeFloat64(buf, kind)
		case "String":
			verifyKind("String", Binary, kind)
			// Put an easter egg here to verify the function is called
			a.String = DecodeString(buf, kind) + "1"
		case "Bool":
			verifyKind("Bool", Boolean, kind)
			a.Bool = DecodeBool(buf, kind)
		case "Time":
			verifyKind("Time", Datetime, kind)
			a.Time = DecodeTime(buf, kind)
		case "Int32":
			verifyKind("Int32", Int, kind)
			a.Int32 = DecodeInt32(buf, kind)
		case "Int":
			verifyKind("Int", Long, kind)
			a.Int = DecodeInt(buf, kind)
		case "Int64":
			verifyKind("Int64", Long, kind)
			a.Int64 = DecodeInt64(buf, kind)
		case "Uint64":
			verifyKind("Uint64", Ulong, kind)
			a.Uint64 = DecodeUint64(buf, kind)
		case "Uint32":
			verifyKind("Uint32", Ulong, kind)
			a.Uint32 = DecodeUint32(buf, kind)
		case "Uint":
			verifyKind("Uint", Ulong, kind)
			a.Uint = DecodeUint(buf, kind)
		case "Map":
		case "Strings":
			verifyKind("Strings", Array, kind)
			a.Strings = DecodeStringArray(buf, kind)
		case "Nil":
			verifyKind("Nil", Null, kind)
		default:
			Skip(buf, kind)
		}
		kind = NextByte(buf)
	}
}

func verifyKind(tag string, want, got byte) {
	if want != got {
		panic(NewBsonError("Decode %s, kind is %v, want %v", tag, got, want))
	}
}

func TestUnmarshalUtil(t *testing.T) {
	a := alltypes{
		Bytes:   []byte("bytes"),
		Float64: float64(64),
		String:  "string",
		Bool:    true,
		Time:    time.Unix(1136243045, 0).UTC(),
		Int64:   int64(-0x8000000000000000),
		Int32:   int32(-0x80000000),
		Int:     int(-0x80000000),
		Uint64:  uint64(0xFFFFFFFFFFFFFFFF),
		Uint32:  uint32(0xFFFFFFFF),
		Uint:    uint(0xFFFFFFFF),
		Strings: []string{"a", "b"},
		Nil:     nil,
	}
	got := verifyMarshal(t, a)
	var out alltypes
	verifyUnmarshal(t, got, &out)
	// Verify easter egg
	if out.String != "string1" {
		t.Errorf("got %s, want %s", out.String, "string1")
	}
	out.String = "string"
	if !reflect.DeepEqual(a, out) {
		t.Errorf("got\n%+v, want\n%+v", out, a)
	}

	b := alltypes{Bytes: []byte(""), Strings: []string{"a"}}
	got = verifyMarshal(t, b)
	var outb alltypes
	verifyUnmarshal(t, got, &outb)
	if outb.Bytes == nil || len(outb.Bytes) != 0 {
		t.Errorf("got %q, want nil", string(outb.Bytes))
	}
}

func TestTypes(t *testing.T) {
	in := map[string]interface{}{
		"bytes":   []byte("bytes"),
		"float64": float64(64),
		"string":  "string",
		"bool":    true,
		"time":    time.Unix(1136243045, 0).UTC(),
		"int64":   int64(-0x8000000000000000),
		"int32":   int32(-0x80000000),
		"int":     int(-0x80000000),
		"uint64":  uint64(0xFFFFFFFFFFFFFFFF),
		"uint32":  uint32(0xFFFFFFFF),
		"uint":    uint(0xFFFFFFFF),
		"slice":   []interface{}{1, nil},
		"nil":     nil,
	}
	marshalled := verifyMarshal(t, in)
	got := make(map[string]interface{})
	verifyUnmarshal(t, marshalled, &got)

	want := map[string]interface{}{
		"bytes":   []byte("bytes"),
		"float64": float64(64),
		"string":  []byte("string"),
		"bool":    true,
		"time":    time.Unix(1136243045, 0).UTC(),
		"int64":   int64(-0x8000000000000000),
		"int32":   int32(-0x80000000),
		"int":     int64(-0x80000000),
		"uint64":  uint64(0xFFFFFFFFFFFFFFFF),
		"uint32":  uint64(0xFFFFFFFF),
		"uint":    uint64(0xFFFFFFFF),
		"slice":   []interface{}{int64(1), nil},
		"nil":     nil,
	}
	// We do the range so the errors are more precise.
	for k, v := range got {
		if !reflect.DeepEqual(v, want[k]) {
			t.Errorf("got \n%+v, want \n%+v", v, want[k])
		}
	}
}

// test that we are calling the right encoding method
// if we use the reflection code, this will fail as reflection
// cannot access the non-exported field
type PrivateStruct struct {
	veryPrivate uint64
}

func (ps *PrivateStruct) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	EncodeOptionalPrefix(buf, Object, key)
	lenWriter := NewLenWriter(buf)

	EncodeUint64(buf, "Type", ps.veryPrivate)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (ps *PrivateStruct) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	VerifyObject(kind)
	Next(buf, 4)

	for kind := NextByte(buf); kind != EOO; kind = NextByte(buf) {
		key := ReadCString(buf)
		switch key {
		case "Type":
			verifyKind("Type", Ulong, kind)
			ps.veryPrivate = DecodeUint64(buf, kind)
		default:
			Skip(buf, kind)
		}
	}
}

// an array can use non-pointers for custom marshaler
type PrivateStructList struct {
	List []PrivateStruct
}

// the map has to be using pointers, so the custom marshaler is used
type PrivateStructMap struct {
	Map map[string]*PrivateStruct
}

type PrivateStructStruct struct {
	Inner *PrivateStruct
}

func TestCustomStruct(t *testing.T) {
	// This should use the custom marshaler & unmarshaler
	s := PrivateStruct{1}
	got := verifyMarshal(t, &s)
	want := "\x13\x00\x00\x00?Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}
	var s2 PrivateStruct
	verifyUnmarshal(t, got, &s2)
	if s2 != s {
		t.Errorf("got \n%+v, want \n%+v", s2, s)
	}

	// This should use the custom marshaler & unmarshaler
	sl := PrivateStructList{make([]PrivateStruct, 1)}
	sl.List[0] = s
	got = verifyMarshal(t, &sl)
	want = "&\x00\x00\x00\x04List\x00\x1b\x00\x00\x00\x030\x00\x13\x00\x00\x00?Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}
	var sl2 PrivateStructList
	verifyUnmarshal(t, got, &sl2)
	if !reflect.DeepEqual(sl2, sl) {
		t.Errorf("got \n%+v, want \n%+v", sl2, sl)
	}

	// This should use the custom marshaler & unmarshaler
	smp := make(map[string]*PrivateStruct)
	smp["first"] = &s
	got = verifyMarshal(t, smp)
	want = "\x1f\x00\x00\x00\x03first\x00\x13\x00\x00\x00?Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}
	smp2 := make(map[string]*PrivateStruct)
	verifyUnmarshal(t, got, &smp2)
	if !reflect.DeepEqual(smp2, smp) {
		t.Errorf("got \n%+v, want \n%+v", smp2, smp)
	}

	// This should not use the custom unmarshaler
	sm := make(map[string]PrivateStruct)
	sm["first"] = s
	sm2 := make(map[string]PrivateStruct)
	verifyUnmarshal(t, got, &sm2)
	if reflect.DeepEqual(sm2, sm) {
		t.Errorf("got \n%+v, want \n%+v", sm2, sm)
	}

	// This should not use the custom marshaler
	got = verifyMarshal(t, sm)
	want = "\x11\x00\x00\x00\x03first\x00\x05\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}

	// This should not use the custom marshaler (or crash)
	nilinner := PrivateStructStruct{}
	got = verifyMarshal(t, &nilinner)
	want = "\f\x00\x00\x00\nInner\x00\x00"
	if string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}
}

type HasPrivate struct {
	private string
	Public  string
}

func TestIgnorePrivateFields(t *testing.T) {
	v := HasPrivate{private: "private", Public: "public"}
	marshaled := verifyMarshal(t, v)
	unmarshaled := new(HasPrivate)
	Unmarshal(marshaled, unmarshaled)
	if unmarshaled.Public != "Public" && unmarshaled.private != "" {
		t.Errorf("private fields were not ignored: %+v", unmarshaled)
	}
}

type LotsMoreFields struct {
	CommonField1 string
	ExtraField1  float64
	ExtraField2  string
	ExtraField3  HasPrivate
	ExtraField4  []string
	CommonField2 string
	ExtraField5  []byte
	ExtraField6  bool
	ExtraField7  time.Time
	ExtraField8  *int
	ExtraField9  int32
	ExtraField10 int64
	ExtraField11 uint64
}

type LotsFewerFields struct {
	CommonField1 string
	CommonField2 string
}

func TestSkipUnknownFields(t *testing.T) {
	v := LotsMoreFields{
		CommonField1: "value1",
		ExtraField1:  1.0,
		ExtraField2:  "abcd",
		ExtraField3:  HasPrivate{private: "private", Public: "public"},
		ExtraField4:  []string{"s1", "s2"},
		CommonField2: "value3",
		ExtraField5:  []byte("abcd"),
		ExtraField6:  true,
		ExtraField7:  time.Now(),
	}
	marshaled := verifyMarshal(t, v)
	unmarshaled := LotsFewerFields{}
	verifyUnmarshal(t, marshaled, &unmarshaled)
	want := LotsFewerFields{
		CommonField1: "value1",
		CommonField2: "value3",
	}
	if unmarshaled != want {
		t.Errorf("got \n%+v, want \n%+v", unmarshaled, want)
	}
}

var testMap map[string]interface{}
var testBlob []byte

func init() {
	testMap = map[string]interface{}{
		"bytes":   []byte("bytes"),
		"float64": float64(64),
		"string":  "string",
		"bool":    true,
		"time":    time.Unix(1136243045, 0),
		"int64":   int64(-0x8000000000000000),
		"int32":   int32(-0x80000000),
		"int":     int(-0x80000000),
		"uint64":  uint64(0xFFFFFFFFFFFFFFFF),
		"uint32":  uint32(0xFFFFFFFF),
		"uint":    uint(0xFFFFFFFF),
		"slice":   []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, nil},
		"nil":     nil,
	}
	testBlob, _ = Marshal(testMap)
}

func BenchmarkMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := Marshal(testMap)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		v := make(map[string]interface{})
		err := Unmarshal(testBlob, &v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func verifyMarshal(t *testing.T, val interface{}) []byte {
	got, err := Marshal(val)
	if err != nil {
		t.Errorf("Marshal error for %+v: %v\n", val, err)
	}
	return got
}

func verifyUnmarshal(t *testing.T, buf []byte, val interface{}) {
	if err := Unmarshal(buf, val); err != nil {
		t.Errorf("Unmarshal error for %+v: %v\n", val, err)
	}
}
