// Copyright 2010 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jsonrpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/rpcplus"
)

type Args struct {
	A, B int
}

type Reply struct {
	C int
}

type Arith int

func (t *Arith) Add(args *Args, reply *Reply) error {
	reply.C = args.A + args.B
	return nil
}

func (t *Arith) Mul(args *Args, reply *Reply) error {
	reply.C = args.A * args.B
	return nil
}

func (t *Arith) Div(args *Args, reply *Reply) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	reply.C = args.A / args.B
	return nil
}

func (t *Arith) Error(args *Args, reply *Reply) error {
	panic("ERROR")
}

func (t *Arith) Thrive(args *Args, sendReply func(reply interface{}) error) error {
	for i := 0; i < args.A; i++ {
		r := &Reply{C: i}
		err := sendReply(r)
		if err != nil {
			return err
		}
	}

	return nil
}

func init() {
	rpcplus.Register(new(Arith))
}

func TestServer(t *testing.T) {
	type addResp struct {
		ID     interface{} `json:"id"`
		Result Reply       `json:"result"`
		Error  interface{} `json:"error"`
	}

	cli, srv := net.Pipe()
	defer cli.Close()
	go ServeConn(srv)
	dec := json.NewDecoder(cli)

	// Send hand-coded requests to server, parse responses.
	for i := 0; i < 10; i++ {
		fmt.Fprintf(cli, `{"method": "Arith.Add", "id": "\u%04d", "params": [{"A": %d, "B": %d}]}`, i, i, i+1)
		var resp addResp
		err := dec.Decode(&resp)
		if err != nil {
			t.Fatalf("Decode: %s", err)
		}
		if resp.Error != nil {
			t.Fatalf("resp.Error: %s", resp.Error)
		}
		if resp.ID.(string) != string(i) {
			t.Fatalf("resp: bad id %q want %q", resp.ID.(string), string(i))
		}
		if resp.Result.C != 2*i+1 {
			t.Fatalf("resp: bad result: %d+%d=%d", i, i+1, resp.Result.C)
		}
	}

	fmt.Fprintf(cli, "{}\n")
	var resp addResp
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("Decode after empty: %s", err)
	}
	if resp.Error == nil {
		t.Fatalf("Expected error, got nil")
	}
}

func TestClient(t *testing.T) {
	ctx := context.Background()
	// Assume server is okay (TestServer is above).
	// Test client against server.
	cli, srv := net.Pipe()
	go ServeConn(srv)

	client := NewClient(cli)
	defer client.Close()

	// Synchronous calls
	args := &Args{7, 8}
	reply := new(Reply)
	err := client.Call(ctx, "Arith.Add", args, reply)
	if err != nil {
		t.Errorf("Add: expected no error but got string %q", err.Error())
	}
	if reply.C != args.A+args.B {
		t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
	}

	args = &Args{7, 8}
	reply = new(Reply)
	err = client.Call(ctx, "Arith.Mul", args, reply)
	if err != nil {
		t.Errorf("Mul: expected no error but got string %q", err.Error())
	}
	if reply.C != args.A*args.B {
		t.Errorf("Mul: expected %d got %d", reply.C, args.A*args.B)
	}

	// Out of order.
	args = &Args{7, 8}
	mulReply := new(Reply)
	mulCall := client.Go(ctx, "Arith.Mul", args, mulReply, nil)
	addReply := new(Reply)
	addCall := client.Go(ctx, "Arith.Add", args, addReply, nil)

	addCall = <-addCall.Done
	if addCall.Error != nil {
		t.Errorf("Add: expected no error but got string %q", addCall.Error.Error())
	}
	if addReply.C != args.A+args.B {
		t.Errorf("Add: expected %d got %d", addReply.C, args.A+args.B)
	}

	mulCall = <-mulCall.Done
	if mulCall.Error != nil {
		t.Errorf("Mul: expected no error but got string %q", mulCall.Error.Error())
	}
	if mulReply.C != args.A*args.B {
		t.Errorf("Mul: expected %d got %d", mulReply.C, args.A*args.B)
	}

	// Error test
	args = &Args{7, 0}
	reply = new(Reply)
	err = client.Call(ctx, "Arith.Div", args, reply)
	// expect an error: zero divide
	if err == nil {
		t.Error("Div: expected error")
	} else if err.Error() != "divide by zero" {
		t.Error("Div: expected divide by zero error; got", err)
	}
}

func TestMalformedInput(t *testing.T) {
	cli, srv := net.Pipe()
	go cli.Write([]byte(`{id:1}`)) // invalid json
	ServeConn(srv)                 // must return, not loop
}

func TestUnexpectedError(t *testing.T) {
	cli, srv := myPipe()
	go cli.PipeWriter.CloseWithError(errors.New("unexpected error!")) // reader will get this error
	ServeConn(srv)                                                    // must return, not loop
}

func TestStreamingCall(t *testing.T) {
	// Assume server is okay (TestServer is above).
	// Test client against server.
	cli, srv := net.Pipe()
	go ServeConn(srv)

	client := NewClient(cli)
	defer client.Close()

	args := &Args{7, 0}
	rowChan := make(chan *Reply, 10)
	c := client.StreamGo("Arith.Thrive", args, rowChan)

	// fetch all the rows
	count := 0
	for row := range rowChan {
		if row.C != count {
			t.Fatal("unexpected value:", row.C)
		}
		count++

		// log.Println("Values: ", row)
	}

	if c.Error != nil {
		t.Fatal("unexpected error:", c.Error.Error())
	}

	if count != 7 {
		t.Fatal("Didn't receive the right number of packets back:", count)
	}
}

// Copied from package net.
func myPipe() (*pipe, *pipe) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	return &pipe{r1, w2}, &pipe{r2, w1}
}

type pipe struct {
	*io.PipeReader
	*io.PipeWriter
}

type pipeAddr int

func (pipeAddr) Network() string {
	return "pipe"
}

func (pipeAddr) String() string {
	return "pipe"
}

func (p *pipe) Close() error {
	err := p.PipeReader.Close()
	err1 := p.PipeWriter.Close()
	if err == nil {
		err = err1
	}
	return err
}

func (p *pipe) LocalAddr() net.Addr {
	return pipeAddr(0)
}

func (p *pipe) RemoteAddr() net.Addr {
	return pipeAddr(0)
}

func (p *pipe) SetTimeout(nsec int64) error {
	return errors.New("net.Pipe does not support timeouts")
}

func (p *pipe) SetReadTimeout(nsec int64) error {
	return errors.New("net.Pipe does not support timeouts")
}

func (p *pipe) SetWriteTimeout(nsec int64) error {
	return errors.New("net.Pipe does not support timeouts")
}
