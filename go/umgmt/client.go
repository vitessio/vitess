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

package umgmt

import (
	"code.google.com/p/vitess/go/relog"
	"errors"
	"io"
	"net"
	"net/rpc"
)

type Client struct {
	*rpc.Client
	conn io.ReadWriteCloser
}

func Dial(address string) (*Client, error) {
	conn, err := net.Dial("unix", address)
	if err != nil {
		return nil, err
	}
	client := new(Client)
	client.conn = conn
	client.Client = rpc.NewClient(conn)
	return client, nil
}

func (client *Client) Close() error {
	if client.conn != nil {
		return client.conn.Close()
	}
	return nil
}

func (client *Client) CloseListeners() error {
	request := new(Request)
	reply := new(Reply)
	err := client.Call("UmgmtService.CloseListeners", request, reply)
	if err != nil {
		relog.Error("rpc err: %v", err)
		return err
	}
	if reply.ErrorCode != 0 {
		relog.Error("CloseListeners err: %v %v", reply.ErrorCode, reply.Message)
		return errors.New(reply.Message)
	}
	return nil
}

func (client *Client) GracefulShutdown() error {
	request := new(Request)
	reply := new(Reply)
	err := client.Call("UmgmtService.GracefulShutdown", request, reply)
	if err != nil {
		relog.Error("rpc err: %v", err)
		return err
	}
	if reply.ErrorCode != 0 {
		relog.Error("GracefulShutdown err: %v %v", reply.ErrorCode, reply.Message)
		return errors.New(reply.Message)
	}
	return nil
}
