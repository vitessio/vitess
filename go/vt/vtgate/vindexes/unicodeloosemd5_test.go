package vindexes

import "testing"

var charVindex Vindex

func init() {
	charVindex, _ = CreateVindex("unicode_loose_md5", "utf8ch", nil)
}

func TestUnicodeLosseMD5Cost(t *testing.T) {
	if charVindex.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", charVindex.Cost())
	}
}

func TestUnicodeLosseMD5(t *testing.T) {
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
		in:  "Test ",
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  " Test",
		out: "\xa2\xe3Q\\~\x8d\xf1\xff\xd2\xcc\xfc\x11Ʊ\x9d\xd1",
	}, {
		in:  "Test\t",
		out: "\x82Em\xd8z\x9cz\x02\xb1\xc2\x05kZ\xba\xa2r",
	}, {
		in:  "TéstLooong",
		out: "\x96\x83\xe1+\x80C\f\xd4S\xf5\xdfߺ\x81ɥ",
	}, {
		in:  "T",
		out: "\xac\x0f\x91y\xf5\x1d\xb8\u007f\xe8\xec\xc0\xcf@ʹz",
	}}
	for _, tcase := range tcases {
		got, err := charVindex.(Unique).Map(nil, []interface{}{[]byte(tcase.in)})
		if err != nil {
			t.Error(err)
		}
		out := string(got[0])
		if out != tcase.out {
			t.Errorf("Map(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
		ok, err := charVindex.Verify(nil, []byte(tcase.in), []byte(tcase.out))
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
		in:  "Test ",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  " Test",
		out: "\x01\t\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Test\t",
		out: "\x18\x16\x16L\x17\xf3\x18\x16\x01\x00",
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
