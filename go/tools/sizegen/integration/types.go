/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//nolint
package integration

type A struct {
	field1 uint64
	field2 uint64
}

type B interface {
	iface()
}

type Bimpl struct {
	field1 uint64
}

func (b *Bimpl) iface() {}

type C struct {
	field1 B
}

type D struct {
	field1 *Bimpl
}

type Padded struct {
	field1 uint64
	field2 uint8
	field3 uint64
}

type Slice1 struct {
	field1 []A
}

type Slice2 struct {
	field1 []B
}

type Slice3 struct {
	field1 []*Bimpl
}

type Map1 struct {
	field1 map[uint8]uint8
}

type Map2 struct {
	field1 map[uint64]A
}

type Map3 struct {
	field1 map[uint64]B
}

type String1 struct {
	field1 string
	field2 uint64
}
