package decimal

import (
	"fmt"
	"testing"
)

func TestDecomposerRoundTrip(t *testing.T) {
	list := []struct {
		N string // Name.
		S string // String value.
		E bool   // Expect an error.
	}{
		{N: "Normal-1", S: "123.456"},
		{N: "Normal-2", S: "-123.456"},
		{N: "NaN-1", S: "NaN"},
		{N: "NaN-2", S: "-NaN"},
		{N: "Infinity-1", S: "Infinity"},
		{N: "Infinity-2", S: "-Infinity"},
	}
	for _, item := range list {
		t.Run(item.N, func(t *testing.T) {
			d := &Big{}
			d, ok := d.SetString(item.S)
			if !ok {
				t.Fatal("unable to set value")
			}
			set, set2 := &Big{}, &Big{}
			f, n, c, e := d.Decompose(nil)
			err := set.Compose(f, n, c, e)
			if err == nil && item.E {
				t.Fatal("expected error, got <nil>")
			}
			err = set2.Compose(f, n, c, e)
			if err == nil && item.E {
				t.Fatal("expected error, got <nil>")
			}
			if set.Cmp(set2) != 0 {
				t.Fatalf("composing the same value twice resulted in different values. set=%v set2=%v", set, set2)
			}

			if err != nil && !item.E {
				t.Fatalf("unexpected error: %v", err)
			}
			if set.Cmp(d) != 0 {
				t.Fatalf("values incorrect, got %v want %v (%s)", set, d, item.S)
			}
		})
	}
}

func TestDecomposerCompose(t *testing.T) {
	list := []struct {
		N string // Name.
		S string // String value.

		Form byte // Form
		Neg  bool
		Coef []byte // Coefficent
		Exp  int32

		Err bool // Expect an error.
	}{
		{N: "Zero", S: "0", Coef: nil, Exp: 0},
		{N: "Normal-1", S: "123.456", Coef: []byte{0x01, 0xE2, 0x40}, Exp: -3},
		{N: "Neg-1", S: "-123.456", Neg: true, Coef: []byte{0x01, 0xE2, 0x40}, Exp: -3},
		{N: "PosExp-1", S: "123456000", Coef: []byte{0x01, 0xE2, 0x40}, Exp: 3},
		{N: "PosExp-2", S: "-123456000", Neg: true, Coef: []byte{0x01, 0xE2, 0x40}, Exp: 3},
		{N: "AllDec-1", S: "0.123456", Coef: []byte{0x01, 0xE2, 0x40}, Exp: -6},
		{N: "AllDec-2", S: "-0.123456", Neg: true, Coef: []byte{0x01, 0xE2, 0x40}, Exp: -6},
		{N: "NaN-1", S: "NaN", Form: 2},
		{N: "Infinity-1", S: "Infinity", Form: 1},
		{N: "Infinity-2", S: "-Infinity", Form: 1, Neg: true},
	}

	for _, item := range list {
		t.Run(item.N, func(t *testing.T) {
			d := &Big{}
			d, ok := d.SetString(item.S)
			if !ok {
				t.Fatal("unable to set value")
			}
			err := d.Compose(item.Form, item.Neg, item.Coef, item.Exp)
			if err != nil && !item.Err {
				t.Fatalf("unexpected error, got %v", err)
			}
			if item.Err {
				if err == nil {
					t.Fatal("expected error, got <nil>")
				}
				return
			}
			if s := fmt.Sprintf("%f", d); s != item.S {
				t.Fatalf("unexpected value, got %q want %q", s, item.S)
			}
		})
	}
}
