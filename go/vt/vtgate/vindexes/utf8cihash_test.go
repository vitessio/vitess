package vindexes

import "testing"

var utf8cihash Vindex

func init() {
	utf8cihash, _ = CreateVindex("utf8cihash", "utf8ch", nil)
}

func TestVarcharHashCost(t *testing.T) {
	if utf8cihash.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", utf8cihash.Cost())
	}
}

func TestVarcharMap(t *testing.T) {
	tcases := []struct {
		in, out string
	}{{
		in:  "Test",
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  "TEST",
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  "Te\u0301st",
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  "Tést",
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  "Bést",
		out: "²3.Os\xd0\aA\x02bIpo/\xb6",
	}, {
		in:  "TéstLooong",
		out: "\x96\x83\xe1+\x80C\f\xd4S\xf5\xdfߺ\x81ɥ",
	}, {
		in:  "T",
		out: "\xac\x0f\x91y\xf5\x1d\xb8\u007f\xe8\xec\xc0\xcf@ʹz",
	}}
	for _, tcase := range tcases {
		got, err := utf8cihash.(Unique).Map(nil, []interface{}{[]byte(tcase.in)})
		if err != nil {
			t.Error(err)
		}
		out := string(got[0])
		if out != tcase.out {
			t.Errorf("Map(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
		ok, err := utf8cihash.Verify(nil, []byte(tcase.in), []byte(tcase.out))
		if err != nil {
			t.Error(err)
		}
		if !ok {
			t.Errorf("Verify(%#v): false, want true", tcase.in)
		}
	}
}

func TestNormalization(t *testing.T) {
	tcases := []struct {
		in, out string
	}{{
		in:  "Test",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "TEST",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Te\u0301st",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Tést",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Bést",
		out: "\x16\x05\x16L\x17\xf3\x18\x16",
	}, {
		in:  "TéstLooong",
		out: "\x18\x16\x16L\x17\xf3\x18\x16\x17\x11\x17q\x17q\x17q\x17O\x16\x91",
	}, {
		in:  "T",
		out: "\x18\x16",
	}}
	for _, tcase := range tcases {
		out := string(normalize([]byte(tcase.in)))
		if out != tcase.out {
			t.Errorf("normalize(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
	}
}
