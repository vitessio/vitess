package json2

import (
	"testing"
)

func TestUnmarshal(t *testing.T) {
	tcases := []struct {
		in, err string
	}{{
		in: `{
  "l2": "val",
  "l3": [
    "l4",
    "l5"asdas"
  ]
}`,
		err: "line: 5, position 9: invalid character 'a' after array element",
	}, {
		in:  "{}",
		err: "",
	}}
	for _, tcase := range tcases {
		out := make(map[string]interface{})
		err := Unmarshal([]byte(tcase.in), &out)
		got := ""
		if err != nil {
			got = err.Error()
		}
		if got != tcase.err {
			t.Errorf("Unmarshal(%v) err: %v, want %v", tcase.in, got, tcase.err)
		}
	}
}
