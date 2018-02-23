// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	pb "google.golang.org/genproto/googleapis/datastore/v1"
	"google.golang.org/grpc"
)

type (
	myBlob   []byte
	myByte   byte
	myString string
)

func makeMyByteSlice(n int) []myByte {
	b := make([]myByte, n)
	for i := range b {
		b[i] = myByte(i)
	}
	return b
}

func makeInt8Slice(n int) []int8 {
	b := make([]int8, n)
	for i := range b {
		b[i] = int8(i)
	}
	return b
}

func makeUint8Slice(n int) []uint8 {
	b := make([]uint8, n)
	for i := range b {
		b[i] = uint8(i)
	}
	return b
}

func newKey(stringID string, parent *Key) *Key {
	return NameKey("kind", stringID, parent)
}

var (
	testKey0     = newKey("name0", nil)
	testKey1a    = newKey("name1", nil)
	testKey1b    = newKey("name1", nil)
	testKey2a    = newKey("name2", testKey0)
	testKey2b    = newKey("name2", testKey0)
	testGeoPt0   = GeoPoint{Lat: 1.2, Lng: 3.4}
	testGeoPt1   = GeoPoint{Lat: 5, Lng: 10}
	testBadGeoPt = GeoPoint{Lat: 1000, Lng: 34}
)

type B0 struct {
	B []byte `datastore:",noindex"`
}

type B1 struct {
	B []int8
}

type B2 struct {
	B myBlob `datastore:",noindex"`
}

type B3 struct {
	B []myByte `datastore:",noindex"`
}

type B4 struct {
	B [][]byte
}

type C0 struct {
	I int
	C chan int
}

type C1 struct {
	I int
	C *chan int
}

type C2 struct {
	I int
	C []chan int
}

type C3 struct {
	C string
}

type c4 struct {
	C string
}

type E struct{}

type G0 struct {
	G GeoPoint
}

type G1 struct {
	G []GeoPoint
}

type K0 struct {
	K *Key
}

type K1 struct {
	K []*Key
}

type S struct {
	St string
}

type NoOmit struct {
	A string
	B int  `datastore:"Bb"`
	C bool `datastore:",noindex"`
}

type OmitAll struct {
	A string `datastore:",omitempty"`
	B int    `datastore:"Bb,omitempty"`
	C bool   `datastore:",omitempty,noindex"`
	F []int  `datastore:",omitempty"`
}

type Omit struct {
	A string `datastore:",omitempty"`
	B int    `datastore:"Bb,omitempty"`
	C bool   `datastore:",omitempty,noindex"`
	F []int  `datastore:",omitempty"`
	S `datastore:",omitempty"`
}

type NoOmits struct {
	No []NoOmit `datastore:",omitempty"`
	S  `datastore:",omitempty"`
	Ss S `datastore:",omitempty"`
}

type N0 struct {
	X0
	Nonymous X0
	Ignore   string `datastore:"-"`
	Other    string
}

type N1 struct {
	X0
	Nonymous []X0
	Ignore   string `datastore:"-"`
	Other    string
}

type N2 struct {
	N1    `datastore:"red"`
	Green N1 `datastore:"green"`
	Blue  N1
	White N1 `datastore:"-"`
}

type N3 struct {
	C3 `datastore:"red"`
}

type N4 struct {
	c4
}

type N5 struct {
	c4 `datastore:"red"`
}

type O0 struct {
	I int64
}

type O1 struct {
	I int32
}

type U0 struct {
	U uint
}

type U1 struct {
	U string
}

type T struct {
	T time.Time
}

type X0 struct {
	S string
	I int
	i int
}

type X1 struct {
	S myString
	I int32
	J int64
}

type X2 struct {
	Z string
	i int
}

type X3 struct {
	S bool
	I int
}

type Y0 struct {
	B bool
	F []float64
	G []float64
}

type Y1 struct {
	B bool
	F float64
}

type Y2 struct {
	B bool
	F []int64
}

type Tagged struct {
	A int   `datastore:"a,noindex"`
	B []int `datastore:"b"`
	C int   `datastore:",noindex"`
	D int   `datastore:""`
	E int
	I int `datastore:"-"`
	J int `datastore:",noindex" json:"j"`

	Y0 `datastore:"-"`
	Z  chan int `datastore:"-"`
}

type InvalidTagged1 struct {
	I int `datastore:"\t"`
}

type InvalidTagged2 struct {
	I int
	J int `datastore:"I"`
}

type InvalidTagged3 struct {
	X string `datastore:"-,noindex"`
}

type InvalidTagged4 struct {
	X string `datastore:",garbage"`
}

type Inner1 struct {
	W int32
	X string
}

type Inner2 struct {
	Y float64
}

type Inner3 struct {
	Z bool
}

type Inner5 struct {
	WW int
}

type Inner4 struct {
	X Inner5
}

type Outer struct {
	A int16
	I []Inner1
	J Inner2
	Inner3
}

type OuterFlatten struct {
	A      int16
	I      []Inner1 `datastore:",flatten"`
	J      Inner2   `datastore:",flatten,noindex"`
	Inner3 `datastore:",flatten"`
	K      Inner4 `datastore:",flatten"`
}

type OuterEquivalent struct {
	A     int16
	IDotW []int32  `datastore:"I.W"`
	IDotX []string `datastore:"I.X"`
	JDotY float64  `datastore:"J.Y"`
	Z     bool
}

type Dotted struct {
	A DottedA `datastore:"A0.A1.A2"`
}

type DottedA struct {
	B DottedB `datastore:"B3"`
}

type DottedB struct {
	C int `datastore:"C4.C5"`
}

type SliceOfSlices struct {
	I int
	S []struct {
		J int
		F []float64
	} `datastore:",flatten"`
}

type Recursive struct {
	I int
	R []Recursive
}

type MutuallyRecursive0 struct {
	I int
	R []MutuallyRecursive1
}

type MutuallyRecursive1 struct {
	I int
	R []MutuallyRecursive0
}

type EntityWithKey struct {
	I int
	S string
	K *Key `datastore:"__key__"`
}

type EntityWithKey2 EntityWithKey

type WithNestedEntityWithKey struct {
	N EntityWithKey
}

type WithNonKeyField struct {
	I int
	K string `datastore:"__key__"`
}

type NestedWithNonKeyField struct {
	N WithNonKeyField
}

type Basic struct {
	A string
}

type PtrToStructField struct {
	B *Basic
	C *Basic `datastore:"c,noindex"`
	*Basic
	D []*Basic
}

var two int = 2

type PtrToInt struct {
	I *int
}

type Doubler struct {
	S string
	I int64
	B bool
}

type Repeat struct {
	Key   string
	Value []byte
}

type Repeated struct {
	Repeats []Repeat
}

func (d *Doubler) Load(props []Property) error {
	return LoadStruct(d, props)
}

func (d *Doubler) Save() ([]Property, error) {
	// Save the default Property slice to an in-memory buffer (a PropertyList).
	props, err := SaveStruct(d)
	if err != nil {
		return nil, err
	}
	var list PropertyList
	if err := list.Load(props); err != nil {
		return nil, err
	}

	// Edit that PropertyList, and send it on.
	for i := range list {
		switch v := list[i].Value.(type) {
		case string:
			// + means string concatenation.
			list[i].Value = v + v
		case int64:
			// + means integer addition.
			list[i].Value = v + v
		}
	}
	return list.Save()
}

var _ PropertyLoadSaver = (*Doubler)(nil)

type Deriver struct {
	S, Derived, Ignored string
}

func (e *Deriver) Load(props []Property) error {
	for _, p := range props {
		if p.Name != "S" {
			continue
		}
		e.S = p.Value.(string)
		e.Derived = "derived+" + e.S
	}
	return nil
}

func (e *Deriver) Save() ([]Property, error) {
	return []Property{
		{
			Name:  "S",
			Value: e.S,
		},
	}, nil
}

var _ PropertyLoadSaver = (*Deriver)(nil)

type BadMultiPropEntity struct{}

func (e *BadMultiPropEntity) Load(props []Property) error {
	return errors.New("unimplemented")
}

func (e *BadMultiPropEntity) Save() ([]Property, error) {
	// Write multiple properties with the same name "I".
	var props []Property
	for i := 0; i < 3; i++ {
		props = append(props, Property{
			Name:  "I",
			Value: int64(i),
		})
	}
	return props, nil
}

var _ PropertyLoadSaver = (*BadMultiPropEntity)(nil)

type testCase struct {
	desc   string
	src    interface{}
	want   interface{}
	putErr string
	getErr string
}

var testCases = []testCase{
	{
		"chan save fails",
		&C0{I: -1},
		&E{},
		"unsupported struct field",
		"",
	},
	{
		"*chan save fails",
		&C1{I: -1},
		&E{},
		"unsupported struct field",
		"",
	},
	{
		"[]chan save fails",
		&C2{I: -1, C: make([]chan int, 8)},
		&E{},
		"unsupported struct field",
		"",
	},
	{
		"chan load fails",
		&C3{C: "not a chan"},
		&C0{},
		"",
		"type mismatch",
	},
	{
		"*chan load fails",
		&C3{C: "not a *chan"},
		&C1{},
		"",
		"type mismatch",
	},
	{
		"[]chan load fails",
		&C3{C: "not a []chan"},
		&C2{},
		"",
		"type mismatch",
	},
	{
		"empty struct",
		&E{},
		&E{},
		"",
		"",
	},
	{
		"geopoint",
		&G0{G: testGeoPt0},
		&G0{G: testGeoPt0},
		"",
		"",
	},
	{
		"geopoint invalid",
		&G0{G: testBadGeoPt},
		&G0{},
		"invalid GeoPoint value",
		"",
	},
	{
		"geopoint as props",
		&G0{G: testGeoPt0},
		&PropertyList{
			Property{Name: "G", Value: testGeoPt0, NoIndex: false},
		},
		"",
		"",
	},
	{
		"geopoint slice",
		&G1{G: []GeoPoint{testGeoPt0, testGeoPt1}},
		&G1{G: []GeoPoint{testGeoPt0, testGeoPt1}},
		"",
		"",
	},
	{
		"omit empty, all",
		&OmitAll{},
		new(PropertyList),
		"",
		"",
	},
	{
		"omit empty",
		&Omit{},
		&PropertyList{
			Property{Name: "St", Value: "", NoIndex: false},
		},
		"",
		"",
	},
	{
		"omit empty, fields populated",
		&Omit{
			A: "a",
			B: 10,
			C: true,
			F: []int{11},
		},
		&PropertyList{
			Property{Name: "A", Value: "a", NoIndex: false},
			Property{Name: "Bb", Value: int64(10), NoIndex: false},
			Property{Name: "C", Value: true, NoIndex: true},
			Property{Name: "F", Value: []interface{}{int64(11)}, NoIndex: false},
			Property{Name: "St", Value: "", NoIndex: false},
		},
		"",
		"",
	},
	{
		"omit empty, fields populated",
		&Omit{
			A: "a",
			B: 10,
			C: true,
			F: []int{11},
			S: S{St: "string"},
		},
		&PropertyList{
			Property{Name: "A", Value: "a", NoIndex: false},
			Property{Name: "Bb", Value: int64(10), NoIndex: false},
			Property{Name: "C", Value: true, NoIndex: true},
			Property{Name: "F", Value: []interface{}{int64(11)}, NoIndex: false},
			Property{Name: "St", Value: "string", NoIndex: false},
		},
		"",
		"",
	},
	{
		"omit empty does not propagate",
		&NoOmits{
			No: []NoOmit{
				NoOmit{},
			},
			S:  S{},
			Ss: S{},
		},
		&PropertyList{
			Property{Name: "No", Value: []interface{}{
				&Entity{
					Properties: []Property{
						Property{Name: "A", Value: "", NoIndex: false},
						Property{Name: "Bb", Value: int64(0), NoIndex: false},
						Property{Name: "C", Value: false, NoIndex: true},
					},
				},
			}, NoIndex: false},
			Property{Name: "Ss", Value: &Entity{
				Properties: []Property{
					Property{Name: "St", Value: "", NoIndex: false},
				},
			}, NoIndex: false},
			Property{Name: "St", Value: "", NoIndex: false},
		},
		"",
		"",
	},
	{
		"key",
		&K0{K: testKey1a},
		&K0{K: testKey1b},
		"",
		"",
	},
	{
		"key with parent",
		&K0{K: testKey2a},
		&K0{K: testKey2b},
		"",
		"",
	},
	{
		"nil key",
		&K0{},
		&K0{},
		"",
		"",
	},
	{
		"all nil keys in slice",
		&K1{[]*Key{nil, nil}},
		&K1{[]*Key{nil, nil}},
		"",
		"",
	},
	{
		"some nil keys in slice",
		&K1{[]*Key{testKey1a, nil, testKey2a}},
		&K1{[]*Key{testKey1b, nil, testKey2b}},
		"",
		"",
	},
	{
		"overflow",
		&O0{I: 1 << 48},
		&O1{},
		"",
		"overflow",
	},
	{
		"time",
		&T{T: time.Unix(1e9, 0)},
		&T{T: time.Unix(1e9, 0)},
		"",
		"",
	},
	{
		"time as props",
		&T{T: time.Unix(1e9, 0)},
		&PropertyList{
			Property{Name: "T", Value: time.Unix(1e9, 0), NoIndex: false},
		},
		"",
		"",
	},
	{
		"uint save",
		&U0{U: 1},
		&U0{},
		"unsupported struct field",
		"",
	},
	{
		"uint load",
		&U1{U: "not a uint"},
		&U0{},
		"",
		"type mismatch",
	},
	{
		"zero",
		&X0{},
		&X0{},
		"",
		"",
	},
	{
		"basic",
		&X0{S: "one", I: 2, i: 3},
		&X0{S: "one", I: 2},
		"",
		"",
	},
	{
		"save string/int load myString/int32",
		&X0{S: "one", I: 2, i: 3},
		&X1{S: "one", I: 2},
		"",
		"",
	},
	{
		"missing fields",
		&X0{S: "one", I: 2, i: 3},
		&X2{},
		"",
		"no such struct field",
	},
	{
		"save string load bool",
		&X0{S: "one", I: 2, i: 3},
		&X3{I: 2},
		"",
		"type mismatch",
	},
	{
		"basic slice",
		&Y0{B: true, F: []float64{7, 8, 9}},
		&Y0{B: true, F: []float64{7, 8, 9}},
		"",
		"",
	},
	{
		"save []float64 load float64",
		&Y0{B: true, F: []float64{7, 8, 9}},
		&Y1{B: true},
		"",
		"requires a slice",
	},
	{
		"save []float64 load []int64",
		&Y0{B: true, F: []float64{7, 8, 9}},
		&Y2{B: true},
		"",
		"type mismatch",
	},
	{
		"single slice is too long",
		&Y0{F: make([]float64, maxIndexedProperties+1)},
		&Y0{},
		"too many indexed properties",
		"",
	},
	{
		"two slices are too long",
		&Y0{F: make([]float64, maxIndexedProperties), G: make([]float64, maxIndexedProperties)},
		&Y0{},
		"too many indexed properties",
		"",
	},
	{
		"one slice and one scalar are too long",
		&Y0{F: make([]float64, maxIndexedProperties), B: true},
		&Y0{},
		"too many indexed properties",
		"",
	},
	{
		"slice of slices of bytes",
		&Repeated{
			Repeats: []Repeat{
				{
					Key:   "key 1",
					Value: []byte("value 1"),
				},
				{
					Key:   "key 2",
					Value: []byte("value 2"),
				},
			},
		},
		&Repeated{
			Repeats: []Repeat{
				{
					Key:   "key 1",
					Value: []byte("value 1"),
				},
				{
					Key:   "key 2",
					Value: []byte("value 2"),
				},
			},
		},
		"",
		"",
	},
	{
		"long blob",
		&B0{B: makeUint8Slice(maxIndexedProperties + 1)},
		&B0{B: makeUint8Slice(maxIndexedProperties + 1)},
		"",
		"",
	},
	{
		"long []int8 is too long",
		&B1{B: makeInt8Slice(maxIndexedProperties + 1)},
		&B1{},
		"too many indexed properties",
		"",
	},
	{
		"short []int8",
		&B1{B: makeInt8Slice(3)},
		&B1{B: makeInt8Slice(3)},
		"",
		"",
	},
	{
		"long myBlob",
		&B2{B: makeUint8Slice(maxIndexedProperties + 1)},
		&B2{B: makeUint8Slice(maxIndexedProperties + 1)},
		"",
		"",
	},
	{
		"short myBlob",
		&B2{B: makeUint8Slice(3)},
		&B2{B: makeUint8Slice(3)},
		"",
		"",
	},
	{
		"long []myByte",
		&B3{B: makeMyByteSlice(maxIndexedProperties + 1)},
		&B3{B: makeMyByteSlice(maxIndexedProperties + 1)},
		"",
		"",
	},
	{
		"short []myByte",
		&B3{B: makeMyByteSlice(3)},
		&B3{B: makeMyByteSlice(3)},
		"",
		"",
	},
	{
		"slice of blobs",
		&B4{B: [][]byte{
			makeUint8Slice(3),
			makeUint8Slice(4),
			makeUint8Slice(5),
		}},
		&B4{B: [][]byte{
			makeUint8Slice(3),
			makeUint8Slice(4),
			makeUint8Slice(5),
		}},
		"",
		"",
	},
	{
		"[]byte must be noindex",
		&PropertyList{
			Property{Name: "B", Value: makeUint8Slice(1501), NoIndex: false},
		},
		nil,
		"[]byte property too long to index",
		"",
	},
	{
		"string must be noindex",
		&PropertyList{
			Property{Name: "B", Value: strings.Repeat("x", 1501), NoIndex: false},
		},
		nil,
		"string property too long to index",
		"",
	},
	{
		"slice of []byte must be noindex",
		&PropertyList{
			Property{Name: "B", Value: []interface{}{
				[]byte("short"),
				makeUint8Slice(1501),
			}, NoIndex: false},
		},
		nil,
		"[]byte property too long to index",
		"",
	},
	{
		"slice of string must be noindex",
		&PropertyList{
			Property{Name: "B", Value: []interface{}{
				"short",
				strings.Repeat("x", 1501),
			}, NoIndex: false},
		},
		nil,
		"string property too long to index",
		"",
	},
	{
		"save tagged load props",
		&Tagged{A: 1, B: []int{21, 22, 23}, C: 3, D: 4, E: 5, I: 6, J: 7},
		&PropertyList{
			// A and B are renamed to a and b; A and C are noindex, I is ignored.
			// Order is sorted as per byName.
			Property{Name: "C", Value: int64(3), NoIndex: true},
			Property{Name: "D", Value: int64(4), NoIndex: false},
			Property{Name: "E", Value: int64(5), NoIndex: false},
			Property{Name: "J", Value: int64(7), NoIndex: true},
			Property{Name: "a", Value: int64(1), NoIndex: true},
			Property{Name: "b", Value: []interface{}{int64(21), int64(22), int64(23)}, NoIndex: false},
		},
		"",
		"",
	},
	{
		"save tagged load tagged",
		&Tagged{A: 1, B: []int{21, 22, 23}, C: 3, D: 4, E: 5, I: 6, J: 7},
		&Tagged{A: 1, B: []int{21, 22, 23}, C: 3, D: 4, E: 5, J: 7},
		"",
		"",
	},
	{
		"invalid tagged1",
		&InvalidTagged1{I: 1},
		&InvalidTagged1{},
		"struct tag has invalid property name",
		"",
	},
	{
		"invalid tagged2",
		&InvalidTagged2{I: 1, J: 2},
		&InvalidTagged2{J: 2},
		"",
		"",
	},
	{
		"invalid tagged3",
		&InvalidTagged3{X: "hello"},
		&InvalidTagged3{},
		"struct tag has invalid property name: \"-\"",
		"",
	},
	{
		"invalid tagged4",
		&InvalidTagged4{X: "hello"},
		&InvalidTagged4{},
		"struct tag has invalid option: \"garbage\"",
		"",
	},
	{
		"doubler",
		&Doubler{S: "s", I: 1, B: true},
		&Doubler{S: "ss", I: 2, B: true},
		"",
		"",
	},
	{
		"save struct load props",
		&X0{S: "s", I: 1},
		&PropertyList{
			Property{Name: "I", Value: int64(1), NoIndex: false},
			Property{Name: "S", Value: "s", NoIndex: false},
		},
		"",
		"",
	},
	{
		"save props load struct",
		&PropertyList{
			Property{Name: "I", Value: int64(1), NoIndex: false},
			Property{Name: "S", Value: "s", NoIndex: false},
		},
		&X0{S: "s", I: 1},
		"",
		"",
	},
	{
		"nil-value props",
		&PropertyList{
			Property{Name: "I", Value: nil, NoIndex: false},
			Property{Name: "B", Value: nil, NoIndex: false},
			Property{Name: "S", Value: nil, NoIndex: false},
			Property{Name: "F", Value: nil, NoIndex: false},
			Property{Name: "K", Value: nil, NoIndex: false},
			Property{Name: "T", Value: nil, NoIndex: false},
			Property{Name: "J", Value: []interface{}{nil, int64(7), nil}, NoIndex: false},
		},
		&struct {
			I int64
			B bool
			S string
			F float64
			K *Key
			T time.Time
			J []int64
		}{
			J: []int64{0, 7, 0},
		},
		"",
		"",
	},
	{
		"save outer load props flatten",
		&OuterFlatten{
			A: 1,
			I: []Inner1{
				{10, "ten"},
				{20, "twenty"},
				{30, "thirty"},
			},
			J: Inner2{
				Y: 3.14,
			},
			Inner3: Inner3{
				Z: true,
			},
			K: Inner4{
				X: Inner5{
					WW: 12,
				},
			},
		},
		&PropertyList{
			Property{Name: "A", Value: int64(1), NoIndex: false},
			Property{Name: "I.W", Value: []interface{}{int64(10), int64(20), int64(30)}, NoIndex: false},
			Property{Name: "I.X", Value: []interface{}{"ten", "twenty", "thirty"}, NoIndex: false},
			Property{Name: "J.Y", Value: float64(3.14), NoIndex: true},
			Property{Name: "K.X.WW", Value: int64(12), NoIndex: false},
			Property{Name: "Z", Value: true, NoIndex: false},
		},
		"",
		"",
	},
	{
		"load outer props flatten",
		&PropertyList{
			Property{Name: "A", Value: int64(1), NoIndex: false},
			Property{Name: "I.W", Value: []interface{}{int64(10), int64(20), int64(30)}, NoIndex: false},
			Property{Name: "I.X", Value: []interface{}{"ten", "twenty", "thirty"}, NoIndex: false},
			Property{Name: "J.Y", Value: float64(3.14), NoIndex: true},
			Property{Name: "Z", Value: true, NoIndex: false},
		},
		&OuterFlatten{
			A: 1,
			I: []Inner1{
				{10, "ten"},
				{20, "twenty"},
				{30, "thirty"},
			},
			J: Inner2{
				Y: 3.14,
			},
			Inner3: Inner3{
				Z: true,
			},
		},
		"",
		"",
	},
	{
		"save outer load props",
		&Outer{
			A: 1,
			I: []Inner1{
				{10, "ten"},
				{20, "twenty"},
				{30, "thirty"},
			},
			J: Inner2{
				Y: 3.14,
			},
			Inner3: Inner3{
				Z: true,
			},
		},
		&PropertyList{
			Property{Name: "A", Value: int64(1), NoIndex: false},
			Property{Name: "I", Value: []interface{}{
				&Entity{
					Properties: []Property{
						Property{Name: "W", Value: int64(10), NoIndex: false},
						Property{Name: "X", Value: "ten", NoIndex: false},
					},
				},
				&Entity{
					Properties: []Property{
						Property{Name: "W", Value: int64(20), NoIndex: false},
						Property{Name: "X", Value: "twenty", NoIndex: false},
					},
				},
				&Entity{
					Properties: []Property{
						Property{Name: "W", Value: int64(30), NoIndex: false},
						Property{Name: "X", Value: "thirty", NoIndex: false},
					},
				},
			}, NoIndex: false},
			Property{Name: "J", Value: &Entity{
				Properties: []Property{
					Property{Name: "Y", Value: float64(3.14), NoIndex: false},
				},
			}, NoIndex: false},
			Property{Name: "Z", Value: true, NoIndex: false},
		},
		"",
		"",
	},
	{
		"save props load outer-equivalent",
		&PropertyList{
			Property{Name: "A", Value: int64(1), NoIndex: false},
			Property{Name: "I.W", Value: []interface{}{int64(10), int64(20), int64(30)}, NoIndex: false},
			Property{Name: "I.X", Value: []interface{}{"ten", "twenty", "thirty"}, NoIndex: false},
			Property{Name: "J.Y", Value: float64(3.14), NoIndex: false},
			Property{Name: "Z", Value: true, NoIndex: false},
		},
		&OuterEquivalent{
			A:     1,
			IDotW: []int32{10, 20, 30},
			IDotX: []string{"ten", "twenty", "thirty"},
			JDotY: 3.14,
			Z:     true,
		},
		"",
		"",
	},
	{
		"dotted names save",
		&Dotted{A: DottedA{B: DottedB{C: 88}}},
		&PropertyList{
			Property{Name: "A0.A1.A2", Value: &Entity{
				Properties: []Property{
					Property{Name: "B3", Value: &Entity{
						Properties: []Property{
							Property{Name: "C4.C5", Value: int64(88), NoIndex: false},
						},
					}, NoIndex: false},
				},
			}, NoIndex: false},
		},
		"",
		"",
	},
	{
		"dotted names load",
		&PropertyList{
			Property{Name: "A0.A1.A2", Value: &Entity{
				Properties: []Property{
					Property{Name: "B3", Value: &Entity{
						Properties: []Property{
							Property{Name: "C4.C5", Value: 99, NoIndex: false},
						},
					}, NoIndex: false},
				},
			}, NoIndex: false},
		},
		&Dotted{A: DottedA{B: DottedB{C: 99}}},
		"",
		"",
	},
	{
		"save struct load deriver",
		&X0{S: "s", I: 1},
		&Deriver{S: "s", Derived: "derived+s"},
		"",
		"",
	},
	{
		"save deriver load struct",
		&Deriver{S: "s", Derived: "derived+s", Ignored: "ignored"},
		&X0{S: "s"},
		"",
		"",
	},
	{
		"zero time.Time",
		&T{T: time.Time{}},
		&T{T: time.Time{}},
		"",
		"",
	},
	{
		"time.Time near Unix zero time",
		&T{T: time.Unix(0, 4e3)},
		&T{T: time.Unix(0, 4e3)},
		"",
		"",
	},
	{
		"time.Time, far in the future",
		&T{T: time.Date(99999, 1, 1, 0, 0, 0, 0, time.UTC)},
		&T{T: time.Date(99999, 1, 1, 0, 0, 0, 0, time.UTC)},
		"",
		"",
	},
	{
		"time.Time, very far in the past",
		&T{T: time.Date(-300000, 1, 1, 0, 0, 0, 0, time.UTC)},
		&T{},
		"time value out of range",
		"",
	},
	{
		"time.Time, very far in the future",
		&T{T: time.Date(294248, 1, 1, 0, 0, 0, 0, time.UTC)},
		&T{},
		"time value out of range",
		"",
	},
	{
		"structs",
		&N0{
			X0:       X0{S: "one", I: 2, i: 3},
			Nonymous: X0{S: "four", I: 5, i: 6},
			Ignore:   "ignore",
			Other:    "other",
		},
		&N0{
			X0:       X0{S: "one", I: 2},
			Nonymous: X0{S: "four", I: 5},
			Other:    "other",
		},
		"",
		"",
	},
	{
		"slice of structs",
		&N1{
			X0: X0{S: "one", I: 2, i: 3},
			Nonymous: []X0{
				{S: "four", I: 5, i: 6},
				{S: "seven", I: 8, i: 9},
				{S: "ten", I: 11, i: 12},
				{S: "thirteen", I: 14, i: 15},
			},
			Ignore: "ignore",
			Other:  "other",
		},
		&N1{
			X0: X0{S: "one", I: 2},
			Nonymous: []X0{
				{S: "four", I: 5},
				{S: "seven", I: 8},
				{S: "ten", I: 11},
				{S: "thirteen", I: 14},
			},
			Other: "other",
		},
		"",
		"",
	},
	{
		"structs with slices of structs",
		&N2{
			N1: N1{
				X0: X0{S: "rouge"},
				Nonymous: []X0{
					{S: "rosso0"},
					{S: "rosso1"},
				},
			},
			Green: N1{
				X0: X0{S: "vert"},
				Nonymous: []X0{
					{S: "verde0"},
					{S: "verde1"},
					{S: "verde2"},
				},
			},
			Blue: N1{
				X0: X0{S: "bleu"},
				Nonymous: []X0{
					{S: "blu0"},
					{S: "blu1"},
					{S: "blu2"},
					{S: "blu3"},
				},
			},
		},
		&N2{
			N1: N1{
				X0: X0{S: "rouge"},
				Nonymous: []X0{
					{S: "rosso0"},
					{S: "rosso1"},
				},
			},
			Green: N1{
				X0: X0{S: "vert"},
				Nonymous: []X0{
					{S: "verde0"},
					{S: "verde1"},
					{S: "verde2"},
				},
			},
			Blue: N1{
				X0: X0{S: "bleu"},
				Nonymous: []X0{
					{S: "blu0"},
					{S: "blu1"},
					{S: "blu2"},
					{S: "blu3"},
				},
			},
		},
		"",
		"",
	},
	{
		"save structs load props",
		&N2{
			N1: N1{
				X0: X0{S: "rouge"},
				Nonymous: []X0{
					{S: "rosso0"},
					{S: "rosso1"},
				},
			},
			Green: N1{
				X0: X0{S: "vert"},
				Nonymous: []X0{
					{S: "verde0"},
					{S: "verde1"},
					{S: "verde2"},
				},
			},
			Blue: N1{
				X0: X0{S: "bleu"},
				Nonymous: []X0{
					{S: "blu0"},
					{S: "blu1"},
					{S: "blu2"},
					{S: "blu3"},
				},
			},
		},
		&PropertyList{
			Property{Name: "Blue", Value: &Entity{
				Properties: []Property{
					Property{Name: "I", Value: int64(0), NoIndex: false},
					Property{Name: "Nonymous", Value: []interface{}{
						&Entity{
							Properties: []Property{
								Property{Name: "I", Value: int64(0), NoIndex: false},
								Property{Name: "S", Value: "blu0", NoIndex: false},
							},
						},
						&Entity{
							Properties: []Property{
								Property{Name: "I", Value: int64(0), NoIndex: false},
								Property{Name: "S", Value: "blu1", NoIndex: false},
							},
						},
						&Entity{
							Properties: []Property{
								Property{Name: "I", Value: int64(0), NoIndex: false},
								Property{Name: "S", Value: "blu2", NoIndex: false},
							},
						},
						&Entity{
							Properties: []Property{
								Property{Name: "I", Value: int64(0), NoIndex: false},
								Property{Name: "S", Value: "blu3", NoIndex: false},
							},
						},
					}, NoIndex: false},
					Property{Name: "Other", Value: "", NoIndex: false},
					Property{Name: "S", Value: "bleu", NoIndex: false},
				},
			}, NoIndex: false},
			Property{Name: "green", Value: &Entity{
				Properties: []Property{
					Property{Name: "I", Value: int64(0), NoIndex: false},
					Property{Name: "Nonymous", Value: []interface{}{
						&Entity{
							Properties: []Property{
								Property{Name: "I", Value: int64(0), NoIndex: false},
								Property{Name: "S", Value: "verde0", NoIndex: false},
							},
						},
						&Entity{
							Properties: []Property{
								Property{Name: "I", Value: int64(0), NoIndex: false},
								Property{Name: "S", Value: "verde1", NoIndex: false},
							},
						},
						&Entity{
							Properties: []Property{
								Property{Name: "I", Value: int64(0), NoIndex: false},
								Property{Name: "S", Value: "verde2", NoIndex: false},
							},
						},
					}, NoIndex: false},
					Property{Name: "Other", Value: "", NoIndex: false},
					Property{Name: "S", Value: "vert", NoIndex: false},
				},
			}, NoIndex: false},
			Property{Name: "red", Value: &Entity{
				Properties: []Property{
					Property{Name: "I", Value: int64(0), NoIndex: false},
					Property{Name: "Nonymous", Value: []interface{}{
						&Entity{
							Properties: []Property{
								Property{Name: "I", Value: int64(0), NoIndex: false},
								Property{Name: "S", Value: "rosso0", NoIndex: false},
							},
						},
						&Entity{
							Properties: []Property{
								Property{Name: "I", Value: int64(0), NoIndex: false},
								Property{Name: "S", Value: "rosso1", NoIndex: false},
							},
						},
					}, NoIndex: false},
					Property{Name: "Other", Value: "", NoIndex: false},
					Property{Name: "S", Value: "rouge", NoIndex: false},
				},
			}, NoIndex: false},
		},
		"",
		"",
	},
	{
		"nested entity with key",
		&WithNestedEntityWithKey{
			N: EntityWithKey{
				I: 12,
				S: "abcd",
				K: testKey0,
			},
		},
		&WithNestedEntityWithKey{
			N: EntityWithKey{
				I: 12,
				S: "abcd",
				K: testKey0,
			},
		},
		"",
		"",
	},
	{
		"entity with key at top level",
		&EntityWithKey{
			I: 12,
			S: "abc",
			K: testKey0,
		},
		&EntityWithKey{
			I: 12,
			S: "abc",
			K: testKey0,
		},
		"",
		"",
	},
	{
		"entity with key at top level (key is populated on load)",
		&EntityWithKey{
			I: 12,
			S: "abc",
		},
		&EntityWithKey{
			I: 12,
			S: "abc",
			K: testKey0,
		},
		"",
		"",
	},
	{
		"__key__ field not a *Key",
		&NestedWithNonKeyField{
			N: WithNonKeyField{
				I: 12,
				K: "abcd",
			},
		},
		&NestedWithNonKeyField{
			N: WithNonKeyField{
				I: 12,
				K: "abcd",
			},
		},
		"datastore: __key__ field on struct datastore.WithNonKeyField is not a *datastore.Key",
		"",
	},
	{
		"save struct with ptr to struct fields",
		&PtrToStructField{
			&Basic{
				A: "b",
			},
			&Basic{
				A: "c",
			},
			&Basic{
				A: "anon",
			},
			[]*Basic{
				&Basic{
					A: "slice0",
				},
				&Basic{
					A: "slice1",
				},
			},
		},
		&PropertyList{
			Property{Name: "A", Value: "anon", NoIndex: false},
			Property{Name: "B", Value: &Entity{
				Properties: []Property{
					Property{Name: "A", Value: "b", NoIndex: false},
				},
			}},
			Property{Name: "D", Value: []interface{}{
				&Entity{
					Properties: []Property{
						Property{Name: "A", Value: "slice0", NoIndex: false},
					},
				},
				&Entity{
					Properties: []Property{
						Property{Name: "A", Value: "slice1", NoIndex: false},
					},
				},
			}, NoIndex: false},
			Property{Name: "c", Value: &Entity{
				Properties: []Property{
					Property{Name: "A", Value: "c", NoIndex: true},
				},
			}, NoIndex: true},
		},
		"",
		"",
	},
	{
		"save and load struct with ptr to struct fields",
		&PtrToStructField{
			&Basic{
				A: "b",
			},
			&Basic{
				A: "c",
			},
			&Basic{
				A: "anon",
			},
			[]*Basic{
				&Basic{
					A: "slice0",
				},
				&Basic{
					A: "slice1",
				},
			},
		},
		&PtrToStructField{
			&Basic{
				A: "b",
			},
			&Basic{
				A: "c",
			},
			&Basic{
				A: "anon",
			},
			[]*Basic{
				&Basic{
					A: "slice0",
				},
				&Basic{
					A: "slice1",
				},
			},
		},
		"",
		"",
	},
	{
		"save struct with pointer to int field",
		&PtrToInt{
			I: &two,
		},
		&PtrToInt{},
		"unsupported struct field",
		"",
	},
	{
		"struct with nil ptr to struct fields",
		&PtrToStructField{
			nil,
			nil,
			nil,
			nil,
		},
		new(PropertyList),
		"",
		"",
	},
	{
		"nested load entity with key",
		&WithNestedEntityWithKey{
			N: EntityWithKey{
				I: 12,
				S: "abcd",
				K: testKey0,
			},
		},
		&PropertyList{
			Property{Name: "N", Value: &Entity{
				Key: testKey0,
				Properties: []Property{
					Property{Name: "I", Value: int64(12), NoIndex: false},
					Property{Name: "S", Value: "abcd", NoIndex: false},
				},
			},
				NoIndex: false},
		},
		"",
		"",
	},
	{
		"nested save entity with key",
		&PropertyList{
			Property{Name: "N", Value: &Entity{
				Key: testKey0,
				Properties: []Property{
					Property{Name: "I", Value: int64(12), NoIndex: false},
					Property{Name: "S", Value: "abcd", NoIndex: false},
				},
			}, NoIndex: false},
		},

		&WithNestedEntityWithKey{
			N: EntityWithKey{
				I: 12,
				S: "abcd",
				K: testKey0,
			},
		},
		"",
		"",
	},
	{
		"anonymous field with tag",
		&N3{
			C3: C3{C: "s"},
		},
		&PropertyList{
			Property{Name: "red", Value: &Entity{
				Properties: []Property{
					Property{Name: "C", Value: "s", NoIndex: false},
				},
			}, NoIndex: false},
		},
		"",
		"",
	},
	{
		"unexported anonymous field",
		&N4{
			c4: c4{C: "s"},
		},
		&PropertyList{
			Property{Name: "C", Value: "s", NoIndex: false},
		},
		"",
		"",
	},
	{
		"unexported anonymous field with tag",
		&N5{
			c4: c4{C: "s"},
		},
		new(PropertyList),
		"",
		"",
	},
	{
		"save props load structs with ragged fields",
		&PropertyList{
			Property{Name: "red.S", Value: "rot", NoIndex: false},
			Property{Name: "green.Nonymous.I", Value: []interface{}{int64(10), int64(11), int64(12), int64(13)}, NoIndex: false},
			Property{Name: "Blue.Nonymous.I", Value: []interface{}{int64(20), int64(21)}, NoIndex: false},
			Property{Name: "Blue.Nonymous.S", Value: []interface{}{"blau0", "blau1", "blau2"}, NoIndex: false},
		},
		&N2{
			N1: N1{
				X0: X0{S: "rot"},
			},
			Green: N1{
				Nonymous: []X0{
					{I: 10},
					{I: 11},
					{I: 12},
					{I: 13},
				},
			},
			Blue: N1{
				Nonymous: []X0{
					{S: "blau0", I: 20},
					{S: "blau1", I: 21},
					{S: "blau2"},
				},
			},
		},
		"",
		"",
	},
	{
		"save structs with noindex tags",
		&struct {
			A struct {
				X string `datastore:",noindex"`
				Y string
			} `datastore:",noindex"`
			B struct {
				X string `datastore:",noindex"`
				Y string
			}
		}{},
		&PropertyList{
			Property{Name: "A", Value: &Entity{
				Properties: []Property{
					Property{Name: "X", Value: "", NoIndex: true},
					Property{Name: "Y", Value: "", NoIndex: true},
				},
			}, NoIndex: true},
			Property{Name: "B", Value: &Entity{
				Properties: []Property{
					Property{Name: "X", Value: "", NoIndex: true},
					Property{Name: "Y", Value: "", NoIndex: false},
				},
			}, NoIndex: false},
		},
		"",
		"",
	},
	{
		"embedded struct with name override",
		&struct {
			Inner1 `datastore:"foo"`
		}{},
		&PropertyList{
			Property{Name: "foo", Value: &Entity{
				Properties: []Property{
					Property{Name: "W", Value: int64(0), NoIndex: false},
					Property{Name: "X", Value: "", NoIndex: false},
				},
			}, NoIndex: false},
		},
		"",
		"",
	},
	{
		"slice of slices",
		&SliceOfSlices{},
		nil,
		"flattening nested structs leads to a slice of slices",
		"",
	},
	{
		"recursive struct",
		&Recursive{},
		&Recursive{},
		"",
		"",
	},
	{
		"mutually recursive struct",
		&MutuallyRecursive0{},
		&MutuallyRecursive0{},
		"",
		"",
	},
	{
		"non-exported struct fields",
		&struct {
			i, J int64
		}{i: 1, J: 2},
		&PropertyList{
			Property{Name: "J", Value: int64(2), NoIndex: false},
		},
		"",
		"",
	},
	{
		"json.RawMessage",
		&struct {
			J json.RawMessage
		}{
			J: json.RawMessage("rawr"),
		},
		&PropertyList{
			Property{Name: "J", Value: []byte("rawr"), NoIndex: false},
		},
		"",
		"",
	},
	{
		"json.RawMessage to myBlob",
		&struct {
			B json.RawMessage
		}{
			B: json.RawMessage("rawr"),
		},
		&B2{B: myBlob("rawr")},
		"",
		"",
	},
	{
		"repeated property names",
		&PropertyList{
			Property{Name: "A", Value: ""},
			Property{Name: "A", Value: ""},
		},
		nil,
		"duplicate Property",
		"",
	},
}

// checkErr returns the empty string if either both want and err are zero,
// or if want is a non-empty substring of err's string representation.
func checkErr(want string, err error) string {
	if err != nil {
		got := err.Error()
		if want == "" || strings.Index(got, want) == -1 {
			return got
		}
	} else if want != "" {
		return fmt.Sprintf("want error %q", want)
	}
	return ""
}

func TestRoundTrip(t *testing.T) {
	for _, tc := range testCases {
		p, err := saveEntity(testKey0, tc.src)
		if s := checkErr(tc.putErr, err); s != "" {
			t.Errorf("%s: save: %s", tc.desc, s)
			continue
		}
		if p == nil {
			continue
		}
		var got interface{}
		if _, ok := tc.want.(*PropertyList); ok {
			got = new(PropertyList)
		} else {
			got = reflect.New(reflect.TypeOf(tc.want).Elem()).Interface()
		}
		err = loadEntity(got, p)
		if s := checkErr(tc.getErr, err); s != "" {
			t.Errorf("%s: load: %s", tc.desc, s)
			continue
		}
		if pl, ok := got.(*PropertyList); ok {
			// Sort by name to make sure we have a deterministic order.
			sortPL(*pl)
		}
		equal := false
		if gotT, ok := got.(*T); ok {
			// Round tripping a time.Time can result in a different time.Location: Local instead of UTC.
			// We therefore test equality explicitly, instead of relying on reflect.DeepEqual.
			equal = gotT.T.Equal(tc.want.(*T).T)
		} else {
			equal = reflect.DeepEqual(got, tc.want)
		}
		if !equal {
			t.Errorf("%s: compare:\ngot:  %#v\nwant: %#v", tc.desc, got, tc.want)
			continue
		}
	}
}

func TestQueryConstruction(t *testing.T) {
	tests := []struct {
		q, exp *Query
		err    string
	}{
		{
			q: NewQuery("Foo"),
			exp: &Query{
				kind:  "Foo",
				limit: -1,
			},
		},
		{
			// Regular filtered query with standard spacing.
			q: NewQuery("Foo").Filter("foo >", 7),
			exp: &Query{
				kind: "Foo",
				filter: []filter{
					{
						FieldName: "foo",
						Op:        greaterThan,
						Value:     7,
					},
				},
				limit: -1,
			},
		},
		{
			// Filtered query with no spacing.
			q: NewQuery("Foo").Filter("foo=", 6),
			exp: &Query{
				kind: "Foo",
				filter: []filter{
					{
						FieldName: "foo",
						Op:        equal,
						Value:     6,
					},
				},
				limit: -1,
			},
		},
		{
			// Filtered query with funky spacing.
			q: NewQuery("Foo").Filter(" foo< ", 8),
			exp: &Query{
				kind: "Foo",
				filter: []filter{
					{
						FieldName: "foo",
						Op:        lessThan,
						Value:     8,
					},
				},
				limit: -1,
			},
		},
		{
			// Filtered query with multicharacter op.
			q: NewQuery("Foo").Filter("foo >=", 9),
			exp: &Query{
				kind: "Foo",
				filter: []filter{
					{
						FieldName: "foo",
						Op:        greaterEq,
						Value:     9,
					},
				},
				limit: -1,
			},
		},
		{
			// Query with ordering.
			q: NewQuery("Foo").Order("bar"),
			exp: &Query{
				kind: "Foo",
				order: []order{
					{
						FieldName: "bar",
						Direction: ascending,
					},
				},
				limit: -1,
			},
		},
		{
			// Query with reverse ordering, and funky spacing.
			q: NewQuery("Foo").Order(" - bar"),
			exp: &Query{
				kind: "Foo",
				order: []order{
					{
						FieldName: "bar",
						Direction: descending,
					},
				},
				limit: -1,
			},
		},
		{
			// Query with an empty ordering.
			q:   NewQuery("Foo").Order(""),
			err: "empty order",
		},
		{
			// Query with a + ordering.
			q:   NewQuery("Foo").Order("+bar"),
			err: "invalid order",
		},
	}
	for i, test := range tests {
		if test.q.err != nil {
			got := test.q.err.Error()
			if !strings.Contains(got, test.err) {
				t.Errorf("%d: error mismatch: got %q want something containing %q", i, got, test.err)
			}
			continue
		}
		if !reflect.DeepEqual(test.q, test.exp) {
			t.Errorf("%d: mismatch: got %v want %v", i, test.q, test.exp)
		}
	}
}

func TestPutMultiTypes(t *testing.T) {
	ctx := context.Background()
	type S struct {
		A int
		B string
	}

	testCases := []struct {
		desc    string
		src     interface{}
		wantErr bool
	}{
		// Test cases to check each of the valid input types for src.
		// Each case has the same elements.
		{
			desc: "type []struct",
			src: []S{
				{1, "one"}, {2, "two"},
			},
		},
		{
			desc: "type []*struct",
			src: []*S{
				{1, "one"}, {2, "two"},
			},
		},
		{
			desc: "type []interface{} with PLS elems",
			src: []interface{}{
				&PropertyList{Property{Name: "A", Value: 1}, Property{Name: "B", Value: "one"}},
				&PropertyList{Property{Name: "A", Value: 2}, Property{Name: "B", Value: "two"}},
			},
		},
		{
			desc: "type []interface{} with struct ptr elems",
			src: []interface{}{
				&S{1, "one"}, &S{2, "two"},
			},
		},
		{
			desc: "type []PropertyLoadSaver{}",
			src: []PropertyLoadSaver{
				&PropertyList{Property{Name: "A", Value: 1}, Property{Name: "B", Value: "one"}},
				&PropertyList{Property{Name: "A", Value: 2}, Property{Name: "B", Value: "two"}},
			},
		},
		{
			desc: "type []P (non-pointer, *P implements PropertyLoadSaver)",
			src: []PropertyList{
				{Property{Name: "A", Value: 1}, Property{Name: "B", Value: "one"}},
				{Property{Name: "A", Value: 2}, Property{Name: "B", Value: "two"}},
			},
		},
		// Test some invalid cases.
		{
			desc: "type []interface{} with struct elems",
			src: []interface{}{
				S{1, "one"}, S{2, "two"},
			},
			wantErr: true,
		},
		{
			desc: "PropertyList",
			src: PropertyList{
				Property{Name: "A", Value: 1},
				Property{Name: "B", Value: "one"},
			},
			wantErr: true,
		},
		{
			desc:    "type []int",
			src:     []int{1, 2},
			wantErr: true,
		},
		{
			desc:    "not a slice",
			src:     S{1, "one"},
			wantErr: true,
		},
	}

	// Use the same keys and expected entities for all tests.
	keys := []*Key{
		NameKey("testKind", "first", nil),
		NameKey("testKind", "second", nil),
	}
	want := []*pb.Mutation{
		{Operation: &pb.Mutation_Upsert{&pb.Entity{
			Key: keyToProto(keys[0]),
			Properties: map[string]*pb.Value{
				"A": {ValueType: &pb.Value_IntegerValue{1}},
				"B": {ValueType: &pb.Value_StringValue{"one"}},
			},
		}}},
		{Operation: &pb.Mutation_Upsert{&pb.Entity{
			Key: keyToProto(keys[1]),
			Properties: map[string]*pb.Value{
				"A": {ValueType: &pb.Value_IntegerValue{2}},
				"B": {ValueType: &pb.Value_StringValue{"two"}},
			},
		}}},
	}

	for _, tt := range testCases {
		// Set up a fake client which captures upserts.
		var got []*pb.Mutation
		client := &Client{
			client: &fakeClient{
				commitFn: func(req *pb.CommitRequest) (*pb.CommitResponse, error) {
					got = req.Mutations
					return &pb.CommitResponse{}, nil
				},
			},
		}

		_, err := client.PutMulti(ctx, keys, tt.src)
		if err != nil {
			if !tt.wantErr {
				t.Errorf("%s: error %v", tt.desc, err)
			}
			continue
		}
		if tt.wantErr {
			t.Errorf("%s: wanted error, but none returned", tt.desc)
			continue
		}
		if len(got) != len(want) {
			t.Errorf("%s: got %d entities, want %d", tt.desc, len(got), len(want))
			continue
		}
		for i, e := range got {
			if !proto.Equal(e, want[i]) {
				t.Logf("%s: entity %d doesn't match\ngot:  %v\nwant: %v", tt.desc, i, e, want[i])
			}
		}
	}
}

func TestNoIndexOnSliceProperties(t *testing.T) {
	// Check that ExcludeFromIndexes is set on the inner elements,
	// rather than the top-level ArrayValue value.
	pl := PropertyList{
		Property{
			Name: "repeated",
			Value: []interface{}{
				123,
				false,
				"short",
				strings.Repeat("a", 1503),
			},
			NoIndex: true,
		},
	}
	key := NameKey("dummy", "dummy", nil)

	entity, err := saveEntity(key, &pl)
	if err != nil {
		t.Fatalf("saveEntity: %v", err)
	}

	want := &pb.Value{
		ValueType: &pb.Value_ArrayValue{&pb.ArrayValue{[]*pb.Value{
			{ValueType: &pb.Value_IntegerValue{123}, ExcludeFromIndexes: true},
			{ValueType: &pb.Value_BooleanValue{false}, ExcludeFromIndexes: true},
			{ValueType: &pb.Value_StringValue{"short"}, ExcludeFromIndexes: true},
			{ValueType: &pb.Value_StringValue{strings.Repeat("a", 1503)}, ExcludeFromIndexes: true},
		}}},
	}
	if got := entity.Properties["repeated"]; !proto.Equal(got, want) {
		t.Errorf("Entity proto differs\ngot:  %v\nwant: %v", got, want)
	}
}

type byName PropertyList

func (s byName) Len() int           { return len(s) }
func (s byName) Less(i, j int) bool { return s[i].Name < s[j].Name }
func (s byName) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// sortPL sorts the property list by property name, and
// recursively sorts any nested property lists, or nested slices of
// property lists.
func sortPL(pl PropertyList) {
	sort.Stable(byName(pl))
	for _, p := range pl {
		switch p.Value.(type) {
		case *Entity:
			sortPL(p.Value.(*Entity).Properties)
		case []interface{}:
			for _, p2 := range p.Value.([]interface{}) {
				if nent, ok := p2.(*Entity); ok {
					sortPL(nent.Properties)
				}
			}
		}
	}
}

func TestValidGeoPoint(t *testing.T) {
	testCases := []struct {
		desc string
		pt   GeoPoint
		want bool
	}{
		{
			"valid",
			GeoPoint{67.21, 13.37},
			true,
		},
		{
			"high lat",
			GeoPoint{-90.01, 13.37},
			false,
		},
		{
			"low lat",
			GeoPoint{90.01, 13.37},
			false,
		},
		{
			"high lng",
			GeoPoint{67.21, 182},
			false,
		},
		{
			"low lng",
			GeoPoint{67.21, -181},
			false,
		},
	}

	for _, tc := range testCases {
		if got := tc.pt.Valid(); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.desc, got, tc.want)
		}
	}
}

func TestPutInvalidEntity(t *testing.T) {
	// Test that trying to put an invalid entity always returns the correct error
	// type.

	// Fake client that can pretend to start a transaction.
	fakeClient := &fakeDatastoreClient{
		beginTransaction: func(*pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error) {
			return &pb.BeginTransactionResponse{
				Transaction: []byte("deadbeef"),
			}, nil
		},
	}
	client := &Client{
		client: fakeClient,
	}

	ctx := context.Background()
	key := IncompleteKey("kind", nil)

	_, err := client.Put(ctx, key, "invalid entity")
	if err != ErrInvalidEntityType {
		t.Errorf("client.Put returned err %v, want %v", err, ErrInvalidEntityType)
	}

	_, err = client.PutMulti(ctx, []*Key{key}, []interface{}{"invalid entity"})
	if me, ok := err.(MultiError); !ok {
		t.Errorf("client.PutMulti returned err %v, want MultiError type", err)
	} else if len(me) != 1 || me[0] != ErrInvalidEntityType {
		t.Errorf("client.PutMulti returned err %v, want MulitError{ErrInvalidEntityType}", err)
	}

	client.RunInTransaction(ctx, func(tx *Transaction) error {
		_, err := tx.Put(key, "invalid entity")
		if err != ErrInvalidEntityType {
			t.Errorf("tx.Put returned err %v, want %v", err, ErrInvalidEntityType)
		}

		_, err = tx.PutMulti([]*Key{key}, []interface{}{"invalid entity"})
		if me, ok := err.(MultiError); !ok {
			t.Errorf("tx.PutMulti returned err %v, want MultiError type", err)
		} else if len(me) != 1 || me[0] != ErrInvalidEntityType {
			t.Errorf("tx.PutMulti returned err %v, want MulitError{ErrInvalidEntityType}", err)
		}

		return errors.New("bang!") // Return error: we don't actually want to commit.
	})
}

type fakeDatastoreClient struct {
	// Optional handlers for the datastore methods.
	// Any handlers left undefined will return an error.
	lookup           func(*pb.LookupRequest) (*pb.LookupResponse, error)
	runQuery         func(*pb.RunQueryRequest) (*pb.RunQueryResponse, error)
	beginTransaction func(*pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error)
	commit           func(*pb.CommitRequest) (*pb.CommitResponse, error)
	rollback         func(*pb.RollbackRequest) (*pb.RollbackResponse, error)
	allocateIds      func(*pb.AllocateIdsRequest) (*pb.AllocateIdsResponse, error)
}

func (c *fakeDatastoreClient) Lookup(ctx context.Context, in *pb.LookupRequest, opts ...grpc.CallOption) (*pb.LookupResponse, error) {
	if c.lookup == nil {
		return nil, errors.New("no lookup handler defined")
	}
	return c.lookup(in)
}
func (c *fakeDatastoreClient) RunQuery(ctx context.Context, in *pb.RunQueryRequest, opts ...grpc.CallOption) (*pb.RunQueryResponse, error) {
	if c.runQuery == nil {
		return nil, errors.New("no runQuery handler defined")
	}
	return c.runQuery(in)
}
func (c *fakeDatastoreClient) BeginTransaction(ctx context.Context, in *pb.BeginTransactionRequest, opts ...grpc.CallOption) (*pb.BeginTransactionResponse, error) {
	if c.beginTransaction == nil {
		return nil, errors.New("no beginTransaction handler defined")
	}
	return c.beginTransaction(in)
}
func (c *fakeDatastoreClient) Commit(ctx context.Context, in *pb.CommitRequest, opts ...grpc.CallOption) (*pb.CommitResponse, error) {
	if c.commit == nil {
		return nil, errors.New("no commit handler defined")
	}
	return c.commit(in)
}
func (c *fakeDatastoreClient) Rollback(ctx context.Context, in *pb.RollbackRequest, opts ...grpc.CallOption) (*pb.RollbackResponse, error) {
	if c.rollback == nil {
		return nil, errors.New("no rollback handler defined")
	}
	return c.rollback(in)
}
func (c *fakeDatastoreClient) AllocateIds(ctx context.Context, in *pb.AllocateIdsRequest, opts ...grpc.CallOption) (*pb.AllocateIdsResponse, error) {
	if c.allocateIds == nil {
		return nil, errors.New("no allocateIds handler defined")
	}
	return c.allocateIds(in)
}
