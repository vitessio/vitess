package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"

	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
)

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}

type rpcHandler struct{}

func (h *rpcHandler) ServeHTTP(c http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		c.Header().Set("Content-Type", "text/plain; charset=utf-8")
		c.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(c, "405 must CONNECT\n")
		return
	}
	conn, _, err := c.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	io.WriteString(conn, "HTTP/1.0 200 Connected to Go RPC\n\n")
	codec := bsonrpc.NewServerCodec(conn)
	rpc.ServeCodec(codec)
}

func main() {
	port := flag.Int("port", 9279, "server port")
	flag.Parse()
	arith := new(Arith)
	rpc.Register(arith)

	http.Handle("/_bson_rpc_", &rpcHandler{})
	addr := fmt.Sprintf("localhost:%d", *port)
	l, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("arithserver running on %s", addr)
	http.Serve(l, nil)
}
