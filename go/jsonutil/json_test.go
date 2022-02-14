package jsonutil

import (
	"testing"
)

func TestMarshalNoEscape(t *testing.T) {
	cases := []struct {
		name     string
		v        interface{}
		expected string
	}{
		{
			name: "normal",
			v: struct {
				Usr string
				Pwd string
			}{
				Usr: "vitess",
				Pwd: "vitess",
			},
			expected: "{\"Usr\":\"vitess\",\"Pwd\":\"vitess\"}",
		},
		{
			name: "not exported",
			v: struct {
				usr string
				pwd string
			}{
				usr: "vitess",
				pwd: "vitess",
			},
			expected: "{}",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			json, _ := MarshalNoEscape(c.v)
			sjson := string(json[:len(json)-1])
			if sjson != c.expected {
				t.Errorf("expected: %v, got: %v", c.expected, sjson)
			}
		})
	}
}

func TestMarshalIndentNoEscape(t *testing.T) {
	cases := []struct {
		name     string
		v        interface{}
		prefix   string
		ident    string
		expected string
	}{
		{
			name: "normal",
			v: struct {
				Usr string
				Pwd string
			}{
				Usr: "vitess",
				Pwd: "vitess",
			},
			prefix:   "test",
			ident:    "\t",
			expected: "{\ntest\t\"Usr\": \"vitess\",\ntest\t\"Pwd\": \"vitess\"\ntest}",
		},
		{
			name: "not exported",
			v: struct {
				usr string
				pwd string
			}{
				usr: "vitess",
				pwd: "vitess",
			},
			expected: "{}",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			json, _ := MarshalIndentNoEscape(c.v, c.prefix, c.ident)
			sjson := string(json[:len(json)-1])
			if sjson != c.expected {
				t.Errorf("expected: %v, got: %v", c.expected, sjson)
			}
		})
	}
}
