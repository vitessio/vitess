// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bsonrpc

import (
	"testing"

	"github.com/youtube/vitess/go/bson"
	rpc "github.com/youtube/vitess/go/rpcplus"
)

type reflectRequestBson struct {
	ServiceMethod string
	Seq           int64
}

type badRequestBson struct {
	Extra         int
	ServiceMethod string
	Seq           int64
}

func TestRequestBson(t *testing.T) {
	reflected, err := bson.Marshal(&reflectRequestBson{
		ServiceMethod: "aa",
		Seq:           1,
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := RequestBson{
		&rpc.Request{
			ServiceMethod: "aa",
			Seq:           1,
		},
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	unmarshalled := RequestBson{Request: new(rpc.Request)}
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if custom.ServiceMethod != unmarshalled.ServiceMethod {
		t.Errorf("want %v, got %#v", custom.ServiceMethod, unmarshalled.ServiceMethod)
	}
	if custom.Seq != unmarshalled.Seq {
		t.Errorf("want %v, got %#v", custom.Seq, unmarshalled.Seq)
	}

	unexpected, err := bson.Marshal(&badRequestBson{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(unexpected, &unmarshalled)
	want = "Unrecognized tag Extra"
	if err == nil || want != err.Error() {
		t.Errorf("want %v, got %v", want, err)
	}
}

type reflectResponseBson struct {
	ServiceMethod string
	Seq           int64
	Error         string
}

type badResponseBson struct {
	Extra         int
	ServiceMethod string
	Seq           int64
	Error         string
}

func TestResponseBson(t *testing.T) {
	reflected, err := bson.Marshal(&reflectResponseBson{
		ServiceMethod: "aa",
		Seq:           1,
		Error:         "err",
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := ResponseBson{
		&rpc.Response{
			ServiceMethod: "aa",
			Seq:           1,
			Error:         "err",
		},
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	unmarshalled := ResponseBson{Response: new(rpc.Response)}
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if custom.ServiceMethod != unmarshalled.ServiceMethod {
		t.Errorf("want %v, got %#v", custom.ServiceMethod, unmarshalled.ServiceMethod)
	}
	if custom.Seq != unmarshalled.Seq {
		t.Errorf("want %v, got %#v", custom.Seq, unmarshalled.Seq)
	}
	if custom.Error != unmarshalled.Error {
		t.Errorf("want %v, got %#v", custom.Error, unmarshalled.Error)
	}

	unexpected, err := bson.Marshal(&badResponseBson{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(unexpected, &unmarshalled)
	want = "Unrecognized tag Extra"
	if err == nil || want != err.Error() {
		t.Errorf("want %v, got %v", want, err)
	}
}
