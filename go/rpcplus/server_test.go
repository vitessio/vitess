// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcplus

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"
)

var (
	newServer                 *Server
	serverAddr, newServerAddr string
	httpServerAddr            string
	once, newOnce, httpOnce   sync.Once
)

const (
	newHttpPath = "/foo"
)

type Args struct {
	A, B int
}

type Reply struct {
	C int
}

type Arith int

// Some of Arith's methods have value args, some have pointer args. That's deliberate.

func (t *Arith) Add(args Args, reply *Reply) error {
	reply.C = args.A + args.B
	return nil
}

func (t *Arith) Mul(args *Args, reply *Reply) error {
	reply.C = args.A * args.B
	return nil
}

func (t *Arith) Div(args Args, reply *Reply) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	reply.C = args.A / args.B
	return nil
}

func (t *Arith) String(args *Args, reply *string) error {
	*reply = fmt.Sprintf("%d+%d=%d", args.A, args.B, args.A+args.B)
	return nil
}

func (t *Arith) Scan(args string, reply *Reply) (err error) {
	_, err = fmt.Sscan(args, &reply.C)
	return
}

func (t *Arith) Error(args *Args, reply *Reply) error {
	panic("ERROR")
}

func (t *Arith) TakesContext(context interface{}, args string, reply *string) error {
	return nil
}

func listenTCP() (net.Listener, string) {
	l, e := net.Listen("tcp", "127.0.0.1:0") // any available address
	if e != nil {
		log.Fatalf("net.Listen tcp :0: %v", e)
	}
	return l, l.Addr().String()
}

func startServer() {
	Register(new(Arith))

	var l net.Listener
	l, serverAddr = listenTCP()
	log.Println("Test RPC server listening on", serverAddr)
	go Accept(l)

	HandleHTTP()
	httpOnce.Do(startHttpServer)
}

func startNewServer() {
	newServer = NewServer()
	newServer.Register(new(Arith))

	var l net.Listener
	l, newServerAddr = listenTCP()
	log.Println("NewServer test RPC server listening on", newServerAddr)
	go Accept(l)

	newServer.HandleHTTP(newHttpPath, "/bar")
	httpOnce.Do(startHttpServer)
}

func startHttpServer() {
	server := httptest.NewServer(nil)
	httpServerAddr = server.Listener.Addr().String()
	log.Println("Test HTTP RPC server listening on", httpServerAddr)
}

func TestRPC(t *testing.T) {
	ctx := context.Background()
	once.Do(startServer)
	testRPC(ctx, t, serverAddr)
	newOnce.Do(startNewServer)
	testRPC(ctx, t, newServerAddr)
}

func testRPC(ctx context.Context, t *testing.T, addr string) {
	client, err := Dial("tcp", addr)
	if err != nil {
		t.Fatal("dialing", err)
	}

	// Synchronous calls
	args := &Args{7, 8}
	reply := new(Reply)
	err = client.Call(ctx, "Arith.Add", args, reply)
	if err != nil {
		t.Errorf("Add: expected no error but got string %q", err.Error())
	}
	if reply.C != args.A+args.B {
		t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
	}

	// Nonexistent method
	args = &Args{7, 0}
	reply = new(Reply)
	err = client.Call(ctx, "Arith.BadOperation", args, reply)
	// expect an error
	if err == nil {
		t.Error("BadOperation: expected error")
	} else if !strings.HasPrefix(err.Error(), "rpc: can't find method ") {
		t.Errorf("BadOperation: expected can't find method error; got %q", err)
	}

	// Unknown service
	args = &Args{7, 8}
	reply = new(Reply)
	err = client.Call(ctx, "Arith.Unknown", args, reply)
	if err == nil {
		t.Error("expected error calling unknown service")
	} else if strings.Index(err.Error(), "method") < 0 {
		t.Error("expected error about method; got", err)
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

	// Bad type.
	reply = new(Reply)
	err = client.Call(ctx, "Arith.Add", reply, reply) // args, reply would be the correct thing to use
	if err == nil {
		t.Error("expected error calling Arith.Add with wrong arg type")
	} else if strings.Index(err.Error(), "type") < 0 {
		t.Error("expected error about type; got", err)
	}

	// Non-struct argument
	const Val = 12345
	str := fmt.Sprint(Val)
	reply = new(Reply)
	err = client.Call(ctx, "Arith.Scan", &str, reply)
	if err != nil {
		t.Errorf("Scan: expected no error but got string %q", err.Error())
	} else if reply.C != Val {
		t.Errorf("Scan: expected %d got %d", Val, reply.C)
	}

	// Non-struct reply
	args = &Args{27, 35}
	str = ""
	err = client.Call(ctx, "Arith.String", args, &str)
	if err != nil {
		t.Errorf("String: expected no error but got string %q", err.Error())
	}
	expect := fmt.Sprintf("%d+%d=%d", args.A, args.B, args.A+args.B)
	if str != expect {
		t.Errorf("String: expected %s got %s", expect, str)
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

	// Takes context
	emptyString := ""
	err = client.Call(ctx, "Arith.TakesContext", "", &emptyString)
	if err != nil {
		t.Errorf("TakesContext: expected no error but got string %q", err.Error())
	}
}

func TestHTTP(t *testing.T) {
	ctx := context.Background()
	once.Do(startServer)
	testHTTPRPC(ctx, t, "")
	newOnce.Do(startNewServer)
	testHTTPRPC(ctx, t, newHttpPath)
}

func testHTTPRPC(ctx context.Context, t *testing.T, path string) {
	var client *Client
	var err error
	if path == "" {
		client, err = DialHTTP("tcp", httpServerAddr)
	} else {
		client, err = DialHTTPPath("tcp", httpServerAddr, path)
	}
	if err != nil {
		t.Fatal("dialing", err)
	}

	// Synchronous calls
	args := &Args{7, 8}
	reply := new(Reply)
	err = client.Call(ctx, "Arith.Add", args, reply)
	if err != nil {
		t.Errorf("Add: expected no error but got string %q", err.Error())
	}
	if reply.C != args.A+args.B {
		t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
	}
}

// CodecEmulator provides a client-like api and a ServerCodec interface.
// Can be used to test ServeRequest.
type CodecEmulator struct {
	server        *Server
	serviceMethod string
	args          *Args
	reply         *Reply
	err           error
}

func (codec *CodecEmulator) Call(ctx context.Context, serviceMethod string, args *Args, reply *Reply) error {
	codec.serviceMethod = serviceMethod
	codec.args = args
	codec.reply = reply
	codec.err = nil
	var serverError error
	if codec.server == nil {
		serverError = ServeRequest(codec)
	} else {
		serverError = codec.server.ServeRequest(codec)
	}
	if codec.err == nil && serverError != nil {
		codec.err = serverError
	}
	return codec.err
}

func (codec *CodecEmulator) ReadRequestHeader(req *Request) error {
	req.ServiceMethod = codec.serviceMethod
	req.Seq = 0
	return nil
}

func (codec *CodecEmulator) ReadRequestBody(argv interface{}) error {
	if codec.args == nil {
		return io.ErrUnexpectedEOF
	}
	*(argv.(*Args)) = *codec.args
	return nil
}

func (codec *CodecEmulator) WriteResponse(resp *Response, reply interface{}, last bool) error {
	if resp.Error != "" {
		codec.err = errors.New(resp.Error)
	} else {
		*codec.reply = *(reply.(*Reply))
	}
	return nil
}

func (codec *CodecEmulator) Close() error {
	return nil
}

func TestServeRequest(t *testing.T) {
	ctx := context.Background()
	once.Do(startServer)
	testServeRequest(ctx, t, nil)
	newOnce.Do(startNewServer)
	testServeRequest(ctx, t, newServer)
}

func testServeRequest(ctx context.Context, t *testing.T, server *Server) {
	client := CodecEmulator{server: server}

	args := &Args{7, 8}
	reply := new(Reply)
	err := client.Call(ctx, "Arith.Add", args, reply)
	if err != nil {
		t.Errorf("Add: expected no error but got string %q", err.Error())
	}
	if reply.C != args.A+args.B {
		t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
	}

	err = client.Call(ctx, "Arith.Add", nil, reply)
	if err == nil {
		t.Errorf("expected error calling Arith.Add with nil arg")
	}
}

type ReplyNotPointer int
type ArgNotPublic int
type ReplyNotPublic int
type local struct{}

func (t *ReplyNotPointer) ReplyNotPointer(args *Args, reply Reply) error {
	return nil
}

func (t *ArgNotPublic) ArgNotPublic(args *local, reply *Reply) error {
	return nil
}

func (t *ReplyNotPublic) ReplyNotPublic(args *Args, reply *local) error {
	return nil
}

// Check that registration handles lots of bad methods and a type with no suitable methods.
func TestRegistrationError(t *testing.T) {
	err := Register(new(ReplyNotPointer))
	if err == nil {
		t.Errorf("expected error registering ReplyNotPointer")
	}
	err = Register(new(ArgNotPublic))
	if err == nil {
		t.Errorf("expected error registering ArgNotPublic")
	}
	err = Register(new(ReplyNotPublic))
	if err == nil {
		t.Errorf("expected error registering ReplyNotPublic")
	}
}

type WriteFailCodec int

func (WriteFailCodec) WriteRequest(*Request, interface{}) error {
	// the panic caused by this error used to not unlock a lock.
	return errors.New("fail")
}

func (WriteFailCodec) ReadResponseHeader(*Response) error {
	select {}
}

func (WriteFailCodec) ReadResponseBody(interface{}) error {
	select {}
}

func (WriteFailCodec) Close() error {
	return nil
}

func TestSendDeadlock(t *testing.T) {
	ctx := context.Background()
	client := NewClientWithCodec(WriteFailCodec(0))

	done := make(chan bool)
	go func() {
		testSendDeadlock(ctx, client)
		testSendDeadlock(ctx, client)
		done <- true
	}()
	select {
	case <-done:
		return
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock")
	}
}

func testSendDeadlock(ctx context.Context, client *Client) {
	defer func() {
		recover()
	}()
	args := &Args{7, 8}
	reply := new(Reply)
	client.Call(ctx, "Arith.Add", args, reply)
}

func dialDirect() (*Client, error) {
	return Dial("tcp", serverAddr)
}

func dialHTTP() (*Client, error) {
	return DialHTTP("tcp", httpServerAddr)
}

func countMallocs(ctx context.Context, dial func() (*Client, error), t *testing.T) uint64 {
	once.Do(startServer)
	client, err := dial()
	if err != nil {
		t.Fatal("error dialing", err)
	}
	args := &Args{7, 8}
	reply := new(Reply)
	memstats := new(runtime.MemStats)
	runtime.ReadMemStats(memstats)
	mallocs := 0 - memstats.Mallocs
	const count = 100
	for i := 0; i < count; i++ {
		err := client.Call(ctx, "Arith.Add", args, reply)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err.Error())
		}
		if reply.C != args.A+args.B {
			t.Errorf("Add: expected %d got %d", reply.C, args.A+args.B)
		}
	}
	runtime.ReadMemStats(memstats)
	mallocs += memstats.Mallocs
	return mallocs / count
}

func TestCountMallocs(t *testing.T) {
	ctx := context.Background()
	fmt.Printf("mallocs per rpc round trip: %d\n", countMallocs(ctx, dialDirect, t))
}

func TestCountMallocsOverHTTP(t *testing.T) {
	ctx := context.Background()
	fmt.Printf("mallocs per HTTP rpc round trip: %d\n", countMallocs(ctx, dialHTTP, t))
}

type writeCrasher struct {
	done chan bool
}

func (writeCrasher) Close() error {
	return nil
}

func (w *writeCrasher) Read(p []byte) (int, error) {
	<-w.done
	return 0, io.EOF
}

func (writeCrasher) Write(p []byte) (int, error) {
	return 0, errors.New("fake write failure")
}

func TestClientWriteError(t *testing.T) {
	ctx := context.Background()
	w := &writeCrasher{done: make(chan bool)}
	client := NewClient(w)
	res := false
	err := client.Call(ctx, "foo", 1, &res)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "fake write failure" {
		t.Error("unexpected value of error:", err)
	}
	w.done <- true
}

func benchmarkEndToEnd(ctx context.Context, dial func() (*Client, error), b *testing.B) {
	b.StopTimer()
	once.Do(startServer)
	client, err := dial()
	if err != nil {
		b.Fatal("error dialing:", err)
	}

	// Synchronous calls
	args := &Args{7, 8}
	procs := runtime.GOMAXPROCS(-1)
	N := int32(b.N)
	var wg sync.WaitGroup
	wg.Add(procs)
	b.StartTimer()

	for p := 0; p < procs; p++ {
		go func() {
			reply := new(Reply)
			for atomic.AddInt32(&N, -1) >= 0 {
				err := client.Call(ctx, "Arith.Add", args, reply)
				if err != nil {
					b.Fatalf("rpc error: Add: expected no error but got string %q", err.Error())
				}
				if reply.C != args.A+args.B {
					b.Fatalf("rpc error: Add: expected %d got %d", reply.C, args.A+args.B)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func benchmarkEndToEndAsync(ctx context.Context, dial func() (*Client, error), b *testing.B) {
	const MaxConcurrentCalls = 100
	b.StopTimer()
	once.Do(startServer)
	client, err := dial()
	if err != nil {
		b.Fatal("error dialing:", err)
	}

	// Asynchronous calls
	args := &Args{7, 8}
	procs := 4 * runtime.GOMAXPROCS(-1)
	send := int32(b.N)
	recv := int32(b.N)
	var wg sync.WaitGroup
	wg.Add(procs)
	gate := make(chan bool, MaxConcurrentCalls)
	res := make(chan *Call, MaxConcurrentCalls)
	b.StartTimer()

	for p := 0; p < procs; p++ {
		go func() {
			for atomic.AddInt32(&send, -1) >= 0 {
				gate <- true
				reply := new(Reply)
				client.Go(ctx, "Arith.Add", args, reply, res)
			}
		}()
		go func() {
			for call := range res {
				A := call.Args.(*Args).A
				B := call.Args.(*Args).B
				C := call.Reply.(*Reply).C
				if A+B != C {
					b.Fatalf("incorrect reply: Add: expected %d got %d", A+B, C)
				}
				<-gate
				if atomic.AddInt32(&recv, -1) == 0 {
					close(res)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkEndToEnd(b *testing.B) {
	benchmarkEndToEnd(context.Background(), dialDirect, b)
}

func BenchmarkEndToEndHTTP(b *testing.B) {
	benchmarkEndToEnd(context.Background(), dialHTTP, b)
}

func BenchmarkEndToEndAsync(b *testing.B) {
	benchmarkEndToEndAsync(context.Background(), dialDirect, b)
}

func BenchmarkEndToEndAsyncHTTP(b *testing.B) {
	benchmarkEndToEndAsync(context.Background(), dialHTTP, b)
}
