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

/*
The micromanagment module provides a tiny server running on a unix domain socket.

It is meant as an alternative to signals for handling graceful server management.
The decision to use unix domain sockets was motivated by future intend to implement
file descriptor passing.

The underlying unix socket acts as a guard for starting up a server.
Once that socket has be acquired it is assumed that previously bound sockets will be
released and startup can continue. You end up delegating execution of your server
initialization to this module via AddStartupCallback().
*/

package umgmt

import (
	"code.google.com/p/vitess/go/relog"
	"container/list"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"sync"
	"syscall"
	"time"
)

const (
	CloseFailed = 1
)

var lameDuckPeriod time.Duration
var rebindDelay time.Duration

type Request struct{}

type Reply struct {
	ErrorCode int
	Message   string
}

type UmgmtService struct {
	mutex sync.Mutex
	// FIXME(msolomon) maybe this is just better as map[*net.Listener] bool
	listeners         list.List
	startupCallbacks  list.List
	shutdownCallbacks list.List
	closeCallbacks    []StartupCallback
	done              chan bool
}

// FIXME(msolomon) seems like RPC should really be registering an interface and something
// that happens to implement it. This might help client-side type safety too.
// type UmgmtService2 interface {
// 	Ping(request *Request, reply *Reply) os.Error
// }

func (service *UmgmtService) addListener(l io.Closer) {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	service.listeners.PushBack(l)
}

type StartupCallback func()
type ShutdownCallback func() error

func (service *UmgmtService) addStartupCallback(f StartupCallback) {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	service.startupCallbacks.PushBack(f)
}

func (service *UmgmtService) addCloseCallback(f StartupCallback) {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	service.closeCallbacks = append(service.closeCallbacks, f)
}

func (service *UmgmtService) addShutdownCallback(f ShutdownCallback) {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	service.shutdownCallbacks.PushBack(f)
}

func (service *UmgmtService) Ping(request *Request, reply *Reply) error {
	relog.Info("ping")
	reply.Message = "pong"
	return nil
}

func (service *UmgmtService) CloseListeners(request *Request, reply *Reply) (err error) {
	// NOTE(msolomon) block this method because we assume that when it returns to the client
	// that there is a very high chance that the listeners have actually closed.
	closeErr := service.closeListeners()
	if closeErr != nil {
		reply.ErrorCode = CloseFailed
		reply.Message = closeErr.Error()
	}
	return
}

func (service *UmgmtService) GracefulShutdown(request *Request, reply *Reply) (err error) {
	// NOTE(msolomon) you can't reliably return from this kind of message, nor can a
	// sane process expect an answer. Do this in a background goroutine and return quickly
	go service.gracefulShutdown()
	return
}

func (service *UmgmtService) closeListeners() (err error) {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	for e := service.listeners.Front(); e != nil; e = e.Next() {
		// NOTE(msolomon) we don't need the whole Listener interface, just Closer
		//relog.Info("closeListeners %T %v", _listener, _listener)
		if listener, ok := e.Value.(io.Closer); ok {
			closeErr := listener.Close()
			if closeErr != nil {
				errMsg := fmt.Sprintf("failed to close listener:%v err:%v", listener, closeErr)
				// just return that at least one error happened, the log will reveal the rest
				err = errors.New(errMsg)
				relog.Error("%s", errMsg)
			}
			// FIXME(msolomon) add a meaningful message telling what listener was closed
		} else {
			relog.Error("bad listener %T %v", listener, listener)
		}
	}
	for _, f := range service.closeCallbacks {
		go f()
	}
	return
}

func (service *UmgmtService) gracefulShutdown() {
	for e := service.shutdownCallbacks.Front(); e != nil; e = e.Next() {
		if callback, ok := e.Value.(ShutdownCallback); ok {
			callbackErr := callback()
			if callbackErr != nil {
				relog.Error("failed running shutdown callback:%v err:%v", callback, callbackErr)
			}
		} else {
			relog.Error("bad callback %T %v", callback, callback)
		}
	}
	service.done <- true
}

func SetLameDuckPeriod(f float32) {
	lameDuckPeriod = time.Duration(f * 1.0e9)
}

func SetRebindDelay(f float32) {
	rebindDelay = time.Duration(f * 1.0e9)
}

func SigTermHandler(signal os.Signal) {
	relog.Info("SigTermHandler")
	defaultService.closeListeners()
	time.Sleep(lameDuckPeriod)
	defaultService.gracefulShutdown()
}

type UmgmtServer struct {
	sync.Mutex
	quit     bool
	listener net.Listener
	connMap  map[net.Conn]bool
}

func (server *UmgmtServer) Serve() error {
	relog.Info("started umgmt server: %v", server.listener.Addr())
	for !server.quit {
		conn, err := server.listener.Accept()
		if err != nil {
			if checkError(err, syscall.EINVAL) {
				if server.quit {
					return nil
				}
				return err
			}
			// syscall.EMFILE, syscall.ENFILE could happen here if you run out of file descriptors
			relog.Error("accept error %v", err)
			continue
		}

		server.Lock()
		server.connMap[conn] = true
		server.Unlock()

		rpc.ServeConn(conn)

		server.Lock()
		delete(server.connMap, conn)
		server.Unlock()
	}
	return nil
}

func (server *UmgmtServer) Close() (err error) {
	server.Lock()
	defer server.Unlock()

	server.quit = true
	if server.listener != nil {
		err = server.listener.Close()
		// NOTE(msolomon) don't worry about the error here, it's not important
		server.listener = nil
	}
	return
}

func (server *UmgmtServer) handleGracefulShutdown() error {
	for conn := range server.connMap {
		conn.Close()
	}
	return nil
}

var defaultService UmgmtService
var DefaultServer *UmgmtServer

func init() {
	defaultService.done = make(chan bool, 1)
}

func ListenAndServe(addr string) error {
	rpc.Register(&defaultService)
	DefaultServer = new(UmgmtServer)
	DefaultServer.connMap = make(map[net.Conn]bool)
	defer DefaultServer.Close()

	var umgmtClient *Client

	for i := 2; i > 0; i-- {
		l, e := net.Listen("unix", addr)
		if e != nil {
			if checkError(e, syscall.EADDRINUSE) {
				var clientErr error
				umgmtClient, clientErr = Dial(addr)
				if clientErr == nil {
					closeErr := umgmtClient.CloseListeners()
					if closeErr != nil {
						relog.Error("closeErr:%v", closeErr)
					}
					// wait for rpc to finish
					if rebindDelay > 0.0 {
						relog.Info("delaying rebind: %vs", rebindDelay)
						time.Sleep(rebindDelay)
					}
					continue
				} else if checkError(clientErr, syscall.ECONNREFUSED) {
					if unlinkErr := syscall.Unlink(addr); unlinkErr != nil {
						relog.Error("can't unlink %v err:%v", addr, unlinkErr)
					}
				} else {
					return e
				}
			} else {
				return e
			}
		} else {
			DefaultServer.listener = l
			break
		}
	}
	if DefaultServer.listener == nil {
		panic("unable to rebind umgmt socket")
	}
	// register the umgmt server itself for dropping - this seems like
	// the common case. i can't see when you *wouldn't* want to drop yourself
	defaultService.addListener(DefaultServer)
	defaultService.addShutdownCallback(func() error {
		return DefaultServer.handleGracefulShutdown()
	})

	// fire off the startup callbacks. if these bind ports, they should
	// call AddListener.
	for e := defaultService.startupCallbacks.Front(); e != nil; e = e.Next() {
		if startupCallback, ok := e.Value.(StartupCallback); ok {
			startupCallback()
		} else {
			relog.Error("bad callback %T %v", e.Value, e.Value)
		}
	}

	if umgmtClient != nil {
		go func() {
			time.Sleep(lameDuckPeriod)
			umgmtClient.GracefulShutdown()
			umgmtClient.Close()
		}()
	}
	err := DefaultServer.Serve()
	// If we exitted gracefully, wait for the service to finish callbacks.
	if err == nil {
		<-defaultService.done
	}
	return err
}

func AddListener(listener io.Closer) {
	defaultService.addListener(listener)
}

func AddShutdownCallback(f ShutdownCallback) {
	defaultService.addShutdownCallback(f)
}

func AddStartupCallback(f StartupCallback) {
	defaultService.addStartupCallback(f)
}

func AddCloseCallback(f StartupCallback) {
	defaultService.addCloseCallback(f)
}

type WrappedError interface {
	Err() error
}

// this is a temporary hack around a few different ways of wrapping
// error codes coming out of the system libraries
func checkError(err, testErr error) bool {
	//relog.Error("checkError %T(%v) == %T(%v)", err, err, testErr, testErr)
	if wrappedError, ok := err.(WrappedError); ok {
		return checkError(wrappedError.Err(), testErr)
	}
	errVal := getField(err, "Err")
	if errVal != nil {
		if osErr, ok := errVal.(error); ok {
			return checkError(osErr, testErr)
		}
	}
	return err == testErr
}

func getField(o interface{}, fieldName string) interface{} {
	val := reflect.Indirect(reflect.ValueOf(o))
	if val.Kind() == reflect.Struct {
		fieldVal := reflect.Indirect(val.FieldByName(fieldName))
		if !fieldVal.IsValid() {
			return nil
		}
		return fieldVal.Interface()
	}
	return nil
}
