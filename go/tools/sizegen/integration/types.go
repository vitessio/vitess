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
