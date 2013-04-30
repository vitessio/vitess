// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The micromanagment module provides a tiny server running on a unix
// domain socket.
//
// It is meant as an alternative to signals for handling graceful
// server management.  The decision to use unix domain sockets was
// motivated by future intend to implement file descriptor passing.
//
// The underlying unix socket acts as a guard for starting up a
// server.  Once that socket has be acquired it is assumed that
// previously bound sockets will be released and startup can
// continue. You must delegate execution of your server initialization
// to this module via AddStartupCallback().

package umgmt

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"

	"code.google.com/p/vitess/go/relog"
)

type Request struct{}

type Reply struct {
	Message string
}

type UmgmtListener interface {
	Close() error
	Addr() net.Addr
}

type UmgmtCallback func()

type UmgmtService struct {
	mutex             sync.Mutex
	listeners         []UmgmtListener
	startupCallbacks  []UmgmtCallback
	shutdownCallbacks []UmgmtCallback
	closeCallbacks    []UmgmtCallback
	done              chan bool

	_lameDuckPeriod time.Duration
	_rebindDelay    time.Duration
}

func newService() *UmgmtService {
	return &UmgmtService{
		listeners:         make([]UmgmtListener, 0, 8),
		startupCallbacks:  make([]UmgmtCallback, 0, 8),
		shutdownCallbacks: make([]UmgmtCallback, 0, 8),
		closeCallbacks:    make([]UmgmtCallback, 0, 8),
		done:              make(chan bool, 1)}
}

func (service *UmgmtService) lameDuckPeriod() time.Duration {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	return service._lameDuckPeriod
}

func (service *UmgmtService) rebindDelay() time.Duration {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	return service._rebindDelay
}

func (service *UmgmtService) addListener(l UmgmtListener) {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	service.listeners = append(service.listeners, l)
}

func (service *UmgmtService) addStartupCallback(f UmgmtCallback) {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	service.startupCallbacks = append(service.startupCallbacks, f)
}

func (service *UmgmtService) addCloseCallback(f UmgmtCallback) {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	service.closeCallbacks = append(service.closeCallbacks, f)
}

func (service *UmgmtService) addShutdownCallback(f UmgmtCallback) {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	service.shutdownCallbacks = append(service.shutdownCallbacks, f)
}

func (service *UmgmtService) Ping(request *Request, reply *Reply) error {
	relog.Info("ping")
	reply.Message = "pong"
	return nil
}

func (service *UmgmtService) CloseListeners(request *Request, reply *Reply) (err error) {
	// NOTE(msolomon) block this method because we assume that when it returns to the client
	// that there is a very high chance that the listeners have actually closed.
	return service.closeListeners()
}

func (service *UmgmtService) closeListeners() (err error) {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	for _, l := range service.listeners {
		addr := l.Addr()
		closeErr := l.Close()
		if closeErr != nil {
			err := fmt.Errorf("failed to close listener on %v err:%v", addr, closeErr)
			// just return that at least one error happened, the log will reveal the rest
			relog.Error("%s", err)
		}
		relog.Info("closed listener %v", addr)
	}
	for _, f := range service.closeCallbacks {
		go f()
	}
	// Prevent duplicate execution.
	service.listeners = service.listeners[:0]
	return
}

func (service *UmgmtService) GracefulShutdown(request *Request, reply *Reply) (err error) {
	// NOTE(msolomon) you can't reliably return from this kind of message, nor can a
	// sane process expect an answer. Do this in a background goroutine and return quickly
	go service.gracefulShutdown()
	return
}

func (service *UmgmtService) gracefulShutdown() {
	service.mutex.Lock()
	defer func() { service.done <- true }()
	defer service.mutex.Unlock()
	for _, f := range service.shutdownCallbacks {
		f()
	}
	// Prevent duplicate execution.
	service.shutdownCallbacks = service.shutdownCallbacks[:0]
}

type UmgmtServer struct {
	sync.Mutex
	quit     bool
	listener net.Listener
	connMap  map[net.Conn]bool
}

// NOTE(msolomon) This function handles requests serially. Multiple clients
// to umgmt doesn't make sense.
func (server *UmgmtServer) Serve() error {
	defer server.listener.Close()
	var tempDelay time.Duration // how long to sleep on accept failure
	relog.Info("started umgmt server: %v", server.listener.Addr())
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				relog.Warning("umgmt: Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}

			server.Lock()
			if server.quit {
				// If we are quitting, an EINVAL is expected.
				err = nil
			}
			server.Unlock()
			return err
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

func (server *UmgmtServer) Addr() net.Addr {
	return server.listener.Addr()
}

func (server *UmgmtServer) Close() error {
	server.Lock()
	defer server.Unlock()

	server.quit = true
	if server.listener != nil {
		return server.listener.Close()
	}
	return nil
}

func (server *UmgmtServer) handleGracefulShutdown() error {
	server.Lock()
	conns := make([]net.Conn, 0, len(server.connMap))
	for conn := range server.connMap {
		conns = append(conns, conn)
	}
	server.Unlock()
	// Closing the connection locks the connMap with an http connection.
	// Operating on a copy of the list is fine for now, but this indicates the locking
	// should be simplified if possible.
	for conn := range server.connMap {
		conn.Close()
	}
	return nil
}

var defaultService = newService()

func ListenAndServe(addr string) error {
	rpc.Register(defaultService)
	server := &UmgmtServer{connMap: make(map[net.Conn]bool)}
	defer func() {
		if err := server.Close(); err != nil {
			relog.Info("umgmt server closed: %v", err)
		}
	}()

	var umgmtClient *Client

	for i := 2; i > 0; i-- {
		l, e := net.Listen("unix", addr)
		if e != nil {
			if umgmtClient != nil {
				umgmtClient.Close()
			}

			if checkError(e, syscall.EADDRINUSE) {
				var clientErr error
				umgmtClient, clientErr = Dial(addr)
				if clientErr == nil {
					closeErr := umgmtClient.CloseListeners()
					if closeErr != nil {
						relog.Error("umgmt CloseListeners err:%v", closeErr)
					}
					// wait for rpc to finish
					rebindDelay := defaultService.rebindDelay()
					if rebindDelay > 0.0 {
						relog.Info("umgmt delaying rebind %v", rebindDelay)
						time.Sleep(rebindDelay)
					}
					continue
				} else if checkError(clientErr, syscall.ECONNREFUSED) {
					relog.Warning("umgmt forced socket removal: %v", addr)
					if rmErr := os.Remove(addr); rmErr != nil {
						relog.Error("umgmt failed removing socket: %v", rmErr)
					}
				} else {
					return e
				}
			} else {
				return e
			}
		} else {
			server.listener = l
			break
		}
	}
	if server.listener == nil {
		return fmt.Errorf("unable to rebind umgmt socket")
	}
	// register the umgmt server itself for dropping - this seems like
	// the common case. i can't see when you *wouldn't* want to drop yourself
	defaultService.addListener(server)
	defaultService.addShutdownCallback(func() {
		server.handleGracefulShutdown()
	})

	// fire off the startup callbacks. if these bind ports, they should
	// call AddListener.
	for _, f := range defaultService.startupCallbacks {
		f()
	}

	if umgmtClient != nil {
		go func() {
			time.Sleep(defaultService.lameDuckPeriod())
			umgmtClient.GracefulShutdown()
			umgmtClient.Close()
		}()
	}
	err := server.Serve()
	// If we exitted gracefully, wait for the service to finish callbacks.
	if err == nil {
		<-defaultService.done
	}
	return err
}

func AddListener(listener UmgmtListener) {
	defaultService.addListener(listener)
}

func AddShutdownCallback(f UmgmtCallback) {
	defaultService.addShutdownCallback(f)
}

func AddStartupCallback(f UmgmtCallback) {
	defaultService.addStartupCallback(f)
}

func AddCloseCallback(f UmgmtCallback) {
	defaultService.addCloseCallback(f)
}

func SetLameDuckPeriod(f float32) {
	defaultService.mutex.Lock()
	defaultService._lameDuckPeriod = time.Duration(f * 1.0e9)
	defaultService.mutex.Unlock()
}

func SetRebindDelay(f float32) {
	defaultService.mutex.Lock()
	defaultService._rebindDelay = time.Duration(f * 1.0e9)
	defaultService.mutex.Unlock()
}

func SigTermHandler(signal os.Signal) {
	relog.Info("SigTermHandler")
	defaultService.closeListeners()
	time.Sleep(defaultService.lameDuckPeriod())
	defaultService.gracefulShutdown()
}

// this is a temporary hack around a few different ways of wrapping
// error codes coming out of the system libraries
func checkError(err, testErr error) bool {
	//relog.Error("checkError %T(%v) == %T(%v)", err, err, testErr, testErr)
	if err == testErr {
		return true
	}

	if opErr, ok := err.(*net.OpError); ok {
		return opErr.Err == testErr
	}

	return false
}
