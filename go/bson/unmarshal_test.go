// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bson

import (
	"reflect"
	"testing"
	"time"
)

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
