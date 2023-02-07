package json

import (
	"testing"
)

func MustParse(j string) *Value {
	var p Parser
	v, err := p.Parse(j)
	if err != nil {
		panic(err)
	}
	return v
}

func TestObjectDelSet(t *testing.T) {
	var p Parser
	var o *Object

	o.Del("xx")

	v, err := p.Parse(`{"fo\no": "bar", "x": [1,2,3]}`)
	if err != nil {
		t.Fatalf("unexpected error during parse: %s", err)
	}
	o, ok := v.Object()
	if !ok {
		t.Fatalf("cannot obtain object")
	}

	// Delete x
	o.Del("x")
	if o.Len() != 1 {
		t.Fatalf("unexpected number of items left; got %d; want %d", o.Len(), 1)
	}

	// Try deleting non-existing value
	o.Del("xxx")
	if o.Len() != 1 {
		t.Fatalf("unexpected number of items left; got %d; want %d", o.Len(), 1)
	}

	// Set new value
	vNew := MustParse(`{"foo":[1,2,3]}`)
	o.Set("new_key", vNew, Set)

	// Delete item with escaped key
	o.Del("fo\no")
	if o.Len() != 1 {
		t.Fatalf("unexpected number of items left; got %d; want %d", o.Len(), 1)
	}

	str := o.String()
	strExpected := `{"new_key": {"foo": [1, 2, 3]}}`
	if str != strExpected {
		t.Fatalf("unexpected string representation for o: got %q; want %q", str, strExpected)
	}

	// Set and Del function as no-op on nil value
	o = nil
	o.Del("x")
	o.Set("x", MustParse(`[3]`), Set)
}

func TestValueDelSet(t *testing.T) {
	var p Parser
	v, err := p.Parse(`{"xx": 123, "x": [1,2,3]}`)
	if err != nil {
		t.Fatalf("unexpected error during parse: %s", err)
	}

	o, _ := v.Object()

	// Delete xx
	o.Del("xx")
	n := o.Len()
	if n != 1 {
		t.Fatalf("unexpected number of items left; got %d; want %d", n, 1)
	}

	// Try deleting non-existing value in the array
	va := o.Get("x")

	// Delete middle element in the array
	va.DelArrayItem(1)

	a, _ := va.Array()
	if len(a) != 2 {
		t.Fatalf("unexpected number of items left in the array; got %d; want %d", len(a), 2)
	}

	/*
		// Update the first element in the array
		vNew := MustParse(`"foobar"`)
		va.Set("0", vNew)

		// Add third element to the array
		vNew = MustParse(`[3]`)
		va.Set("3", vNew)

		// Add invalid array index to the array
		va.Set("invalid", MustParse(`"nonsense"`))

		str := v.String()
		strExpected := `{"x":["foobar",3,null,[3]]}`
		if str != strExpected {
			t.Fatalf("unexpected string representation for o: got %q; want %q", str, strExpected)
		}

		// Set and Del function as no-op on nil value
		v = nil
		v.Del("x")
		v.Set("x", MustParse(`[]`))
		v.SetArrayItem(1, MustParse(`[]`))
	*/
}
