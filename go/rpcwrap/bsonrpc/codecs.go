/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package bsonrpc

import (
	"bytes"
	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/rpcwrap"
	"io"
	"net/rpc"
)

const (
	codecName = "bson"
)

type ClientCodec struct {
	rwc io.ReadWriteCloser
}

func NewClientCodec(conn io.ReadWriteCloser) rpc.ClientCodec {
	return &ClientCodec{conn}
}

const DefaultBufferSize = 4096

func (self *ClientCodec) WriteRequest(r *rpc.Request, body interface{}) error {
	buf := bytes.NewBuffer(make([]byte, 0, DefaultBufferSize))
	if err := bson.MarshalToBuffer(buf, &RequestBson{r}); err != nil {
		return err
	}
	if err := bson.MarshalToBuffer(buf, body); err != nil {
		return err
	}
	_, err := buf.WriteTo(self.rwc)
	return err
}

func (self *ClientCodec) ReadResponseHeader(r *rpc.Response) error {
	return bson.UnmarshalFromStream(self.rwc, &ResponseBson{r})
}

func (self *ClientCodec) ReadResponseBody(body interface{}) error {
	return bson.UnmarshalFromStream(self.rwc, body)
}

func (self *ClientCodec) Close() error {
	return self.rwc.Close()
}

type ServerCodec struct {
	rwc io.ReadWriteCloser
}

func NewServerCodec(conn io.ReadWriteCloser) rpc.ServerCodec {
	return &ServerCodec{conn}
}

func (self *ServerCodec) ReadRequestHeader(r *rpc.Request) error {
	return bson.UnmarshalFromStream(self.rwc, &RequestBson{r})
}

func (self *ServerCodec) ReadRequestBody(body interface{}) error {
	return bson.UnmarshalFromStream(self.rwc, body)
}

func (self *ServerCodec) WriteResponse(r *rpc.Response, body interface{}) error {
	buf := bytes.NewBuffer(make([]byte, 0, DefaultBufferSize))
	if err := bson.MarshalToBuffer(buf, &ResponseBson{r}); err != nil {
		return err
	}
	if err := bson.MarshalToBuffer(buf, body); err != nil {
		return err
	}
	_, err := buf.WriteTo(self.rwc)
	return err
}

func (self *ServerCodec) Close() error {
	return self.rwc.Close()
}

func DialHTTP(network, address string) (*rpc.Client, error) {
	return rpcwrap.DialHTTP(network, address, codecName, NewClientCodec)
}

func ServeRPC() {
	rpcwrap.ServeRPC(codecName, NewServerCodec)
}

func ServeHTTP() {
	rpcwrap.ServeHTTP(codecName, NewServerCodec)
}
