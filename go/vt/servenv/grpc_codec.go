/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package servenv

import (
	"vitess.io/vitess/go/vt/servenv/vtproto"

	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
)

// Name is the name registered for the proto compressor.
const Name = "proto"

type vtprotoCodec struct{}

func (vtprotoCodec) Marshal(v interface{}) ([]byte, error) {
	return vtproto.Marshal(v)
}

func (vtprotoCodec) Unmarshal(data []byte, v interface{}) error {
	return vtproto.Unmarshal(data, v)
}

func (vtprotoCodec) Name() string {
	return Name
}

func init() {
	encoding.RegisterCodec(vtprotoCodec{})
}
