package main

import (
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

func Test_transformArgsForPflag(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.String("foobar", "baz", "")
	fs.StringP("name", "n", "", "")
	fs.BoolP("debug", "d", true, "")

	tests := []struct {
		args        []string
		transformed []string
	}{
		{
			args:        []string{"--foobar=hello", "--name", "myname", "-d"},
			transformed: []string{"--foobar=hello", "--name", "myname", "-d"},
		},
		{
			args:        []string{"-foobar=hello", "-name", "myname", "-d"},
			transformed: []string{"--foobar=hello", "--name", "myname", "-d"},
		},
		{
			args:        []string{"--", "-foobar=hello"},
			transformed: []string{"--", "-foobar=hello"},
		},
		{
			args:        []string{"-dn"}, // combined shortopts
			transformed: []string{"-dn"},
		},
	}

	for _, tt := range tests {
		tt := tt
		name := strings.Join(tt.args, " ")

		t.Run(name, func(t *testing.T) {
			got := transformArgsForPflag(fs, tt.args)
			assert.Equal(t, tt.transformed, got)
		})
	}
}
