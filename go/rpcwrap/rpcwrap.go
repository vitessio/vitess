// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcwrap

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/auth"
	"github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/stats"
)

const (
	connected = "200 Connected to Go RPC"
)

var (
	connCount    = stats.NewInt("connection-count")
	connAccepted = stats.NewInt("connection-accepted")
)

type ClientCodecFactory func(conn io.ReadWriteCloser) rpc.ClientCodec

type BufferedConnection struct {
	isClosed bool
	*bufio.Reader
	io.WriteCloser
}

func NewBufferedConnection(conn io.ReadWriteCloser) *BufferedConnection {
	connCount.Add(1)
	connAccepted.Add(1)
	return &BufferedConnection{false, bufio.NewReader(conn), conn}
}

// FIXME(sougou/szopa): Find a better way to track connection count.
func (bc *BufferedConnection) Close() error {
	if !bc.isClosed {
		bc.isClosed = true
		connCount.Add(-1)
	}
	return bc.WriteCloser.Close()
}

// DialHTTP connects to a go HTTP RPC server using the specified codec.
// use 0 as connectTimeout for no timeout
// use nil as config to not use TLS
func DialHTTP(network, address, codecName string, cFactory ClientCodecFactory, connectTimeout time.Duration, config *tls.Config) (*rpc.Client, error) {
	return dialHTTP(network, address, codecName, cFactory, false, connectTimeout, config)
}

// DialAuthHTTP connects to an authenticated go HTTP RPC server using
// the specified codec and credentials.
// use 0 as connectTimeout for no timeout
// use nil as config to not use TLS
func DialAuthHTTP(network, address, user, password, codecName string, cFactory ClientCodecFactory, connectTimeout time.Duration, config *tls.Config) (conn *rpc.Client, err error) {
	if conn, err = dialHTTP(network, address, codecName, cFactory, true, connectTimeout, config); err != nil {
		return
	}
	reply := new(auth.GetNewChallengeReply)
	if err = conn.Call(context.TODO(), "AuthenticatorCRAMMD5.GetNewChallenge", "", reply); err != nil {
		return
	}
	proof := auth.CRAMMD5GetExpected(user, password, reply.Challenge)

	if err = conn.Call(
		context.TODO(),
		"AuthenticatorCRAMMD5.Authenticate",
		auth.AuthenticateRequest{Proof: proof}, new(auth.AuthenticateReply)); err != nil {
		return
	}
	return
}

func dialHTTP(network, address, codecName string, cFactory ClientCodecFactory, auth bool, connectTimeout time.Duration, config *tls.Config) (*rpc.Client, error) {
	var err error
	var conn net.Conn
	if connectTimeout != 0 {
		conn, err = net.DialTimeout(network, address, connectTimeout)
	} else {
		conn, err = net.Dial(network, address)
	}
	if err != nil {
		return nil, err
	}

	if config != nil {
		conn = tls.Client(conn, config)
	}

	_, err = io.WriteString(conn, "CONNECT "+GetRpcPath(codecName, auth)+" HTTP/1.0\n\n")
	if err != nil {
		return nil, err
	}

	// Require successful HTTP response
	// before switching to RPC protocol.
	buffered := NewBufferedConnection(conn)
	resp, err := http.ReadResponse(buffered.Reader, &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return rpc.NewClientWithCodec(cFactory(buffered)), nil
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	conn.Close()
	return nil, &net.OpError{Op: "dial-http", Net: network + " " + address, Addr: nil, Err: err}
}

type ServerCodecFactory func(conn io.ReadWriteCloser) rpc.ServerCodec

// ServeRPC handles rpc requests using the hijack scheme of rpc
func ServeRPC(codecName string, cFactory ServerCodecFactory) {
	http.Handle(GetRpcPath(codecName, false), &rpcHandler{cFactory, rpc.DefaultServer, false})
}

// ServeAuthRPC handles authenticated rpc requests using the hijack
// scheme of rpc
func ServeAuthRPC(codecName string, cFactory ServerCodecFactory) {
	http.Handle(GetRpcPath(codecName, true), &rpcHandler{cFactory, AuthenticatedServer, true})
}

// ServeCustomRPC serves the given rpc requests with the provided ServeMux,
// authenticated or not
func ServeCustomRPC(handler *http.ServeMux, server *rpc.Server, useAuth bool, codecName string, cFactory ServerCodecFactory) {
	handler.Handle(GetRpcPath(codecName, useAuth), &rpcHandler{cFactory, server, useAuth})
}

// AuthenticatedServer is an rpc.Server instance that serves
// authenticated calls.
var AuthenticatedServer = rpc.NewServer()

// rpcHandler handles rpc queries for a 'CONNECT' method.
type rpcHandler struct {
	cFactory ServerCodecFactory
	server   *rpc.Server
	useAuth  bool
}

// ServeHTTP implements http.Handler's ServeHTTP
func (h *rpcHandler) ServeHTTP(c http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		c.Header().Set("Content-Type", "text/plain; charset=utf-8")
		c.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(c, "405 must CONNECT\n")
		return
	}
	conn, _, err := c.(http.Hijacker).Hijack()
	if err != nil {
		log.Errorf("rpc hijacking %s: %v", req.RemoteAddr, err)
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	codec := h.cFactory(NewBufferedConnection(conn))
	ctx := proto.NewContext(req.RemoteAddr)
	if h.useAuth {
		if authenticated, err := auth.Authenticate(ctx, codec); !authenticated {
			if err != nil {
				log.Errorf("authentication erred at %s: %v", req.RemoteAddr, err)
			}
			codec.Close()
			return
		}
	}
	h.server.ServeCodecWithContext(ctx, codec)
}

// GetRpcPath returns the toplevel path used for serving RPCs over HTTP
func GetRpcPath(codecName string, auth bool) string {
	path := "/_" + codecName + "_rpc_"
	if auth {
		path += "/auth"
	}
	return path
}

// ServeCustomRPC serves the given http rpc requests with the provided ServeMux,
// does not support built-in authentication
func ServeHTTPRPC(
	handler *http.ServeMux,
	server *rpc.Server,
	codecName string,
	cFactory ServerCodecFactory,
	contextCreator func(*http.Request) context.Context) {

	handler.Handle(
		GetRpcPath(codecName, false),
		&httpRpcHandler{
			cFactory:       cFactory,
			server:         server,
			contextCreator: contextCreator,
		},
	)
}

// httpRpcHandler handles rpc queries for a all types of HTTP requests, does not
// maintain a persistent connection.
type httpRpcHandler struct {
	cFactory ServerCodecFactory
	server   *rpc.Server
	// contextCreator creates an application specific context, while creating
	// the context it should not read the request body nor write anything to
	// headers
	contextCreator func(*http.Request) context.Context
}

// ServeHTTP implements http.Handler's ServeHTTP
func (h *httpRpcHandler) ServeHTTP(c http.ResponseWriter, req *http.Request) {
	codec := h.cFactory(NewBufferedConnection(
		&httpReadWriteCloser{rw: c, req: req},
	))

	var ctx context.Context

	if h.contextCreator != nil {
		ctx = h.contextCreator(req)
	} else {
		ctx = proto.NewContext(req.RemoteAddr)
	}

	h.server.ServeCodecWithContextOnce(
		new(sync.Mutex),
		false,
		ctx,
		codec,
	)

	codec.Close()
}

// httpReadWriteCloser wraps http.ResponseWriter and http.Request, with the help
// of those, implements ReadWriteCloser interface
type httpReadWriteCloser struct {
	rw  http.ResponseWriter
	req *http.Request
}

// Read implements Reader interface
func (i *httpReadWriteCloser) Read(p []byte) (n int, err error) {
	return i.req.Body.Read(p)
}

// Write implements Writer interface
func (i *httpReadWriteCloser) Write(p []byte) (n int, err error) {
	return i.rw.Write(p)
}

// Close implements Closer interface
func (i *httpReadWriteCloser) Close() error {
	return i.req.Body.Close()
}
