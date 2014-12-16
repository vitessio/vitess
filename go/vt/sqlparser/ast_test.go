// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import "testing"

func TestWhere(t *testing.T) {
	var w *Where
	buf := NewTrackedBuffer(nil)
	w.Format(buf)
	if buf.String() != "" {
		t.Errorf("w.Format(nil): %q, want \"\"", buf.String)
	}
	w = NewWhere(AST_WHERE, nil)
	buf = NewTrackedBuffer(nil)
	w.Format(buf)
	if buf.String() != "" {
		t.Errorf("w.Format(&Where{nil}: %q, want \"\"", buf.String)
	}
}

func TestLimits(t *testing.T) {
	var l *Limit
	o, r, err := l.Limits()
	if o != nil || r != nil || err != nil {
		t.Errorf("got %v, %v, %v, want nils", o, r, err)
	}

	l = &Limit{Offset: NumVal([]byte("aa"))}
	_, _, err = l.Limits()
	wantErr := "strconv.ParseInt: parsing \"aa\": invalid syntax"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %s", err, wantErr)
	}

	l = &Limit{Offset: NumVal([]byte("2"))}
	_, _, err = l.Limits()
	wantErr = "unexpected node for rowcount: <nil>"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %s", err, wantErr)
	}

	l = &Limit{Offset: StrVal([]byte("2"))}
	_, _, err = l.Limits()
	wantErr = "unexpected node for offset: [50]"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %s", err, wantErr)
	}

	l = &Limit{Offset: NumVal([]byte("2")), Rowcount: NumVal([]byte("aa"))}
	_, _, err = l.Limits()
	wantErr = "strconv.ParseInt: parsing \"aa\": invalid syntax"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %s", err, wantErr)
	}

	l = &Limit{Offset: NumVal([]byte("2")), Rowcount: NumVal([]byte("3"))}
	o, r, err = l.Limits()
	if o.(int64) != 2 || r.(int64) != 3 || err != nil {
		t.Errorf("got %v %v %v, want 2, 3, nil", o, r, err)
	}

	l = &Limit{Offset: ValArg([]byte(":a")), Rowcount: NumVal([]byte("3"))}
	o, r, err = l.Limits()
	if o.(string) != ":a" || r.(int64) != 3 || err != nil {
		t.Errorf("got %v %v %v, want :a, 3, nil", o, r, err)
	}

	l = &Limit{Offset: nil, Rowcount: NumVal([]byte("3"))}
	o, r, err = l.Limits()
	if o != nil || r.(int64) != 3 || err != nil {
		t.Errorf("got %v %v %v, want nil, 3, nil", o, r, err)
	}

	l = &Limit{Offset: nil, Rowcount: ValArg([]byte(":a"))}
	o, r, err = l.Limits()
	if o != nil || r.(string) != ":a" || err != nil {
		t.Errorf("got %v %v %v, want nil, :a, nil", o, r, err)
	}

	l = &Limit{Offset: NumVal([]byte("-2")), Rowcount: NumVal([]byte("0"))}
	_, _, err = l.Limits()
	wantErr = "negative offset: -2"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %s", err, wantErr)
	}

	l = &Limit{Offset: NumVal([]byte("2")), Rowcount: NumVal([]byte("-2"))}
	_, _, err = l.Limits()
	wantErr = "negative limit: -2"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %s", err, wantErr)
	}
}
