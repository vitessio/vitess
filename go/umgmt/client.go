// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package umgmt

import (
	"io"
	"net"
	"net/rpc"

	"code.google.com/p/vitess/go/relog"
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

func (client *Client) Ping() (string, error) {
	request := new(Request)
	reply := new(Reply)
	err := client.Call("UmgmtService.Ping", request, reply)
	if err != nil {
		relog.Error("rpc err: %v", err)
		return "ERROR", err
	}
	return reply.Message, nil
}

func (client *Client) CloseListeners() error {
	request := new(Request)
	reply := new(Reply)
	err := client.Call("UmgmtService.CloseListeners", request, reply)
	if err != nil {
		relog.Error("CloseListeners err: %v", err)
	}
	return err
}

func (client *Client) GracefulShutdown() error {
	request := new(Request)
	reply := new(Reply)
	err := client.Call("UmgmtService.GracefulShutdown", request, reply)
	if err != nil && err != io.ErrUnexpectedEOF {
		relog.Error("GracefulShutdown err: %v", err)
	}
	return err
}
