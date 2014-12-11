// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bsonrpc

import (
	"testing"

	"github.com/henryanand/vitess/go/bson"
	rpc "github.com/henryanand/vitess/go/rpcplus"
)

type reflectRequestBson struct {
	ServiceMethod string
	Seq           uint64
}

type extraRequestBson struct {
	Extra         int
	ServiceMethod string
	Seq           uint64
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

	extra, err := bson.Marshal(&extraRequestBson{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}

type reflectResponseBson struct {
	ServiceMethod string
	Seq           uint64
	Error         string
}

type extraResponseBson struct {
	Extra         int
	ServiceMethod string
	Seq           uint64
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

	extra, err := bson.Marshal(&extraResponseBson{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}
