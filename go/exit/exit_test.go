package exit

import (
	"testing"
)

type repanicType int

func TestReturn(t *testing.T) {
	defer func() {
		err := recover()
		if err == nil {
			t.Errorf("Return() did not panic with exit code")
		}

		switch code := err.(type) {
		case exitCode:
			if code != 152 {
				t.Errorf("got %v, want %v", code, 152)
			}
		default:
			panic(err)
		}
	}()

	Return(152)
}

func TestRecover(t *testing.T) {
	var code int

	exitFunc = func(c int) {
		code = c
	}

	func() {
		defer Recover()
		Return(8235)
	}()

	if code != 8235 {
		t.Errorf("got %v, want %v", code, 8235)
	}
}

func TestRecoverRepanic(t *testing.T) {
	defer func() {
		err := recover()

		if err == nil {
			t.Errorf("Recover() didn't re-panic an error other than exitCode")
			return
		}

		if _, ok := err.(repanicType); !ok {
			panic(err) // something unexpected went wrong
		}
	}()

	defer Recover()

	panic(repanicType(1))
}

func TestRecoverAll(t *testing.T) {
	exitFunc = func(int) {}

	defer func() {
		err := recover()

		if err != nil {
			t.Errorf("RecoverAll() didn't absorb all panics")
		}
	}()

	defer RecoverAll()

	panic(repanicType(1))
}

// TestRecoverNil checks that Recover() does nothing when there is no panic.
func TestRecoverNil(t *testing.T) {
	defer Recover()
}
