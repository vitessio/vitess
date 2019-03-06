package vterrors

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/net/context"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestWrapNil(t *testing.T) {
	got := Wrap(nil, "no error")
	if got != nil {
		t.Errorf("Wrap(nil, \"no error\"): got %#v, expected nil", got)
	}
}

func TestWrap(t *testing.T) {
	tests := []struct {
		err         error
		message     string
		wantMessage string
		wantCode    vtrpcpb.Code
	}{
		{io.EOF, "read error", "read error: EOF", vtrpcpb.Code_UNKNOWN},
		{New(vtrpcpb.Code_ALREADY_EXISTS, "oops"), "client error", "client error: oops", vtrpcpb.Code_ALREADY_EXISTS},
	}

	for _, tt := range tests {
		got := Wrap(tt.err, tt.message)
		if got.Error() != tt.wantMessage {
			t.Errorf("Wrap(%v, %q): got: [%v], want [%v]", tt.err, tt.message, got, tt.wantMessage)
		}
		if Code(got) != tt.wantCode {
			t.Errorf("Wrap(%v, %v): got: [%v], want [%v]", tt.err, tt, Code(got), tt.wantCode)
		}
	}
}

type nilError struct{}

func (nilError) Error() string { return "nil error" }

func TestRootCause(t *testing.T) {
	x := New(vtrpcpb.Code_FAILED_PRECONDITION, "error")
	tests := []struct {
		err  error
		want error
	}{{
		// nil error is nil
		err:  nil,
		want: nil,
	}, {
		// explicit nil error is nil
		err:  (error)(nil),
		want: nil,
	}, {
		// typed nil is nil
		err:  (*nilError)(nil),
		want: (*nilError)(nil),
	}, {
		// uncaused error is unaffected
		err:  io.EOF,
		want: io.EOF,
	}, {
		// caused error returns cause
		err:  Wrap(io.EOF, "ignored"),
		want: io.EOF,
	}, {
		err:  x, // return from errors.New
		want: x,
	}}

	for i, tt := range tests {
		got := RootCause(tt.err)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("test %d: got %#v, want %#v", i+1, got, tt.want)
		}
	}
}

func TestCause(t *testing.T) {
	x := New(vtrpcpb.Code_FAILED_PRECONDITION, "error")
	tests := []struct {
		err  error
		want error
	}{{
		// nil error is nil
		err:  nil,
		want: nil,
	}, {
		// uncaused error is nil
		err:  io.EOF,
		want: nil,
	}, {
		// caused error returns cause
		err:  Wrap(io.EOF, "ignored"),
		want: io.EOF,
	}, {
		err:  x, // return from errors.New
		want: nil,
	}}

	for i, tt := range tests {
		got := Cause(tt.err)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("test %d: got %#v, want %#v", i+1, got, tt.want)
		}
	}
}

func TestWrapfNil(t *testing.T) {
	got := Wrapf(nil, "no error")
	if got != nil {
		t.Errorf("Wrapf(nil, \"no error\"): got %#v, expected nil", got)
	}
}

func TestWrapf(t *testing.T) {
	tests := []struct {
		err     error
		message string
		want    string
	}{
		{io.EOF, "read error", "read error: EOF"},
		{Wrapf(io.EOF, "read error without format specifiers"), "client error", "client error: read error without format specifiers: EOF"},
		{Wrapf(io.EOF, "read error with %d format specifier", 1), "client error", "client error: read error with 1 format specifier: EOF"},
	}

	for _, tt := range tests {
		got := Wrapf(tt.err, tt.message).Error()
		if got != tt.want {
			t.Errorf("Wrapf(%v, %q): got: %v, want %v", tt.err, tt.message, got, tt.want)
		}
	}
}

func TestErrorf(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{Errorf(vtrpcpb.Code_DATA_LOSS, "read error without format specifiers"), "read error without format specifiers"},
		{Errorf(vtrpcpb.Code_DATA_LOSS, "read error with %d format specifier", 1), "read error with 1 format specifier"},
	}

	for _, tt := range tests {
		got := tt.err.Error()
		if got != tt.want {
			t.Errorf("Errorf(%v): got: %q, want %q", tt.err, got, tt.want)
		}
	}
}

func innerMost() error {
	return Wrap(io.ErrNoProgress, "oh noes")
}

func middle() error {
	return innerMost()
}

func outer() error {
	return middle()
}

func TestStackFormat(t *testing.T) {
	err := outer()
	got := fmt.Sprintf("%v", err)

	assertStringContains(t, got, "innerMost")
	assertStringContains(t, got, "middle")
	assertStringContains(t, got, "outer")
}

func assertStringContains(t *testing.T, s, substring string) {
	if !strings.Contains(s, substring) {
		t.Errorf("string did not contain `%v`: \n %v", substring, s)
	}
}

// errors.New, etc values are not expected to be compared by value
// but the change in errors#27 made them incomparable. Assert that
// various kinds of errors have a functional equality operator, even
// if the result of that equality is always false.
func TestErrorEquality(t *testing.T) {
	vals := []error{
		nil,
		io.EOF,
		errors.New("EOF"),
		New(vtrpcpb.Code_ALREADY_EXISTS, "EOF"),
		Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "EOF"),
		Wrap(io.EOF, "EOF"),
		Wrapf(io.EOF, "EOF%d", 2),
	}

	for i := range vals {
		for j := range vals {
			_ = vals[i] == vals[j] // mustn't panic
		}
	}
}

func TestCreation(t *testing.T) {
	testcases := []struct {
		in, want vtrpcpb.Code
	}{{
		in:   vtrpcpb.Code_CANCELED,
		want: vtrpcpb.Code_CANCELED,
	}, {
		in:   vtrpcpb.Code_UNKNOWN,
		want: vtrpcpb.Code_UNKNOWN,
	}}
	for _, tcase := range testcases {
		if got := Code(New(tcase.in, "")); got != tcase.want {
			t.Errorf("Code(New(%v)): %v, want %v", tcase.in, got, tcase.want)
		}
		if got := Code(Errorf(tcase.in, "")); got != tcase.want {
			t.Errorf("Code(Errorf(%v)): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}

func TestCode(t *testing.T) {
	testcases := []struct {
		in   error
		want vtrpcpb.Code
	}{{
		in:   nil,
		want: vtrpcpb.Code_OK,
	}, {
		in:   errors.New("generic"),
		want: vtrpcpb.Code_UNKNOWN,
	}, {
		in:   New(vtrpcpb.Code_CANCELED, "generic"),
		want: vtrpcpb.Code_CANCELED,
	}, {
		in:   context.Canceled,
		want: vtrpcpb.Code_CANCELED,
	}, {
		in:   context.DeadlineExceeded,
		want: vtrpcpb.Code_DEADLINE_EXCEEDED,
	}}
	for _, tcase := range testcases {
		if got := Code(tcase.in); got != tcase.want {
			t.Errorf("Code(%v): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}

func TestWrapping(t *testing.T) {
	err1 := Errorf(vtrpcpb.Code_UNAVAILABLE, "foo")
	err2 := Wrapf(err1, "bar")
	err3 := Wrapf(err2, "baz")
	errorWithStack := fmt.Sprintf("%v", err3)

	assertEquals(t, err3.Error(), "baz: bar: foo")
	assertContains(t, errorWithStack, "foo")
	assertContains(t, errorWithStack, "bar")
	assertContains(t, errorWithStack, "baz")
	assertContains(t, errorWithStack, "TestWrapping")
}

func assertContains(t *testing.T, s, substring string) {
	if !strings.Contains(s, substring) {
		t.Fatalf("expected string that contains [%s] but got [%s]", substring, s)
	}
}

func assertEquals(t *testing.T, a, b interface{}) {
	if a != b {
		t.Fatalf("expected [%s] to be equal to [%s]", a, b)
	}
}