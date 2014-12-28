package rpcwrap

import (
	"errors"
	"log"
	"net"
	"net/http"

	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcplus/jsonrpc"
	"golang.org/x/net/context"

	"testing"
)

type Request struct {
	A, B int
}

type Arith int

func (t *Arith) Success(ctx context.Context, args *Request, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Fail(ctx context.Context, args *Request, reply *int) error {
	return errors.New("fail")
}

func startListening() net.Listener {
	server := rpcplus.NewServer()
	server.Register(new(Arith))

	mux := http.NewServeMux()

	contextCreator := func(req *http.Request) context.Context {
		return context.Background()
	}

	ServeHTTPRPC(
		mux,                    // httpmuxer
		server,                 // rpcserver
		"json",                 // codec name
		jsonrpc.NewServerCodec, // jsoncodec
		contextCreator,         // contextCreator
	)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}

	go http.Serve(l, mux)
	return l
}

func createAddr(l net.Listener) string {
	return "http://" + l.Addr().String() + GetRpcPath("json", false)
}

func TestSuccess(t *testing.T) {
	l := startListening()
	defer l.Close()

	params := &Request{
		A: 7,
		B: 8,
	}

	var r int

	err := jsonrpc.NewHTTPClient(createAddr(l)).Call("Arith.Success", params, &r)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r != 56 {
		t.Fatalf("Expected: 56, but got: %d", r)
	}
}

func TestFail(t *testing.T) {
	l := startListening()
	defer l.Close()

	params := &Request{
		A: 7,
		B: 8,
	}

	var r int

	err := jsonrpc.NewHTTPClient(createAddr(l)).Call("Arith.Fail", params, &r)
	if err == nil {
		t.Fatal("Expected a non-nil err")
	}

	if err.Error() != "fail" {
		t.Fatalf("Expected \"fail\" as err message, but got %s", err.Error())
	}

	if r != 0 {
		t.Fatalf("Expected: 0, but got: %d", r)
	}
}
