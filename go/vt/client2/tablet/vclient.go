// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This implements some additional error handling logic to make the client
// more robust in the face of transient problems with easy solutions.
package tablet

import (
	"fmt"
	"net"
	"strings"
	"time"

	"code.google.com/p/vitess/go/db"
	"code.google.com/p/vitess/go/relog"
)

const (
	ErrTypeFatal = 1 //errors.New("vt: fatal: reresolve endpoint")
	ErrTypeRetry = 2 //errors.New("vt: retry: reconnect endpoint")
	ErrTypeApp   = 3 //errors.New("vt: app level error")
)

const (
	DefaultReconnectDelay = 2 * time.Millisecond
	DefaultMaxAttempts    = 2
	DefaultTimeout        = 30 * time.Second
)

var zeroTime time.Time

// Layer some logic on top of the basic tablet protocol to support
// fast retry when we can.

type VtConn struct {
	Conn
	maxAttempts    int           // How many times should try each retriable operation?
	timeout        time.Duration // How long should we wait for a given operation?
	timeFailed     time.Time     // This is the time a client transitioned from presumable health to failure.
	reconnectDelay time.Duration
}

// How long should we wait to try to recover?
// FIXME(msolomon) not sure if maxAttempts is still useful
func (vtc *VtConn) recoveryTimeout() time.Duration {
	return vtc.timeout * 2
}

func (vtc *VtConn) handleErr(err error) (int, error) {
	now := time.Now()
	if vtc.timeFailed.IsZero() {
		vtc.timeFailed = now
	} else if now.Sub(vtc.timeFailed) > vtc.recoveryTimeout() {
		vtc.Close()
		return ErrTypeFatal, fmt.Errorf("vt: max recovery time exceeded: %v", err)
	}

	errType := ErrTypeApp
	if tabletErr, ok := err.(TabletError); ok {
		msg := strings.ToLower(tabletErr.err.Error())
		if strings.HasPrefix(msg, "fatal") {
			errType = ErrTypeFatal
		} else if strings.HasPrefix(msg, "retry") {
			errType = ErrTypeRetry
		}
	} else if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
		errType = ErrTypeRetry
	}

	if errType == ErrTypeRetry && vtc.TransactionId != 0 {
		errType = ErrTypeApp
		err = fmt.Errorf("vt: cannot retry within a transaction: %v", err)
		time.Sleep(vtc.reconnectDelay)
		vtc.Close()
		dialErr := vtc.dial()
		relog.Warning("vt: redial error %v", dialErr)
	}

	return errType, err
}

func (vtc *VtConn) Exec(query string, bindVars map[string]interface{}) (db.Result, error) {
	attempt := 0
	for {
		result, err := vtc.Conn.Exec(query, bindVars)
		if err == nil {
			vtc.timeFailed = zeroTime
			return result, nil
		}

		errType, err := vtc.handleErr(err)
		if errType != ErrTypeRetry {
			return nil, err
		}
		for {
			attempt++
			if attempt > vtc.maxAttempts {
				return nil, fmt.Errorf("vt: max recovery attempts exceeded: %v", err)
			}
			vtc.Close()
			time.Sleep(vtc.reconnectDelay)
			if err := vtc.dial(); err == nil {
				break
			}
			relog.Warning("vt: error dialing on exec %v", vtc.Conn.dbi.Host)
		}
	}

	panic("unreachable")
}

func (vtc *VtConn) Begin() (db.Tx, error) {
	attempt := 0
	for {
		tx, err := vtc.Conn.Begin()
		if err == nil {
			vtc.timeFailed = zeroTime
			return tx, nil
		}

		errType, err := vtc.handleErr(err)
		if errType != ErrTypeRetry {
			return nil, err
		}
		for {
			attempt++
			if attempt > vtc.maxAttempts {
				return nil, fmt.Errorf("vt: max recovery attempts exceeded: %v", err)
			}
			vtc.Close()
			time.Sleep(vtc.reconnectDelay)
			if err := vtc.dial(); err == nil {
				break
			}
			relog.Warning("vt: error dialing on begin %v", vtc.Conn.dbi.Host)
		}
	}
	panic("unreachable")
}

func (vtc *VtConn) Commit() (err error) {
	if err = vtc.Conn.Commit(); err == nil {
		vtc.timeFailed = zeroTime
		return nil
	}

	// Not much we can do at this point, just annotate the error and return.
	_, err = vtc.handleErr(err)
	return err
}

func DialVtdb(dbi string, stream bool, timeout time.Duration) (*VtConn, error) {
	url, err := parseDbi(dbi)
	if err != nil {
		return nil, err
	}
	conn := &VtConn{
		Conn:           Conn{dbi: url, stream: stream},
		maxAttempts:    DefaultMaxAttempts,
		timeout:        timeout,
		reconnectDelay: DefaultReconnectDelay,
	}

	if err := conn.dial(); err != nil {
		return nil, err
	}
	return conn, nil
}

type vDriver struct {
	stream bool
}

func (driver *vDriver) Open(name string) (db.Conn, error) {
	return DialVtdb(name, driver.stream, DefaultTimeout)
}

func init() {
	db.Register("vttablet", &vDriver{})
	db.Register("vttablet-streaming", &vDriver{true})
}
