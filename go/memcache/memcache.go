// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package memcache

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type Connection struct {
	conn     net.Conn
	buffered bufio.ReadWriter
	timeout  time.Duration
}

type Result struct {
	Key   string
	Value []byte
	Flags uint16
	Cas   uint64
}

func Connect(address string, timeout time.Duration) (conn *Connection, err error) {
	var network string
	if strings.Contains(address, "/") {
		network = "unix"
	} else {
		network = "tcp"
	}
	var nc net.Conn
	nc, err = net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, err
	}
	return &Connection{
		conn: nc,
		buffered: bufio.ReadWriter{
			Reader: bufio.NewReader(nc),
			Writer: bufio.NewWriter(nc),
		},
		timeout: timeout,
	}, nil
}

func (mc *Connection) Close() {
	if mc.conn == nil {
		return
	}
	mc.conn.Close()
	mc.conn = nil
}

func (mc *Connection) IsClosed() bool {
	return mc.conn == nil
}

func (mc *Connection) Get(keys ...string) (results []Result, err error) {
	defer handleError(&err)
	results = mc.get("get", keys)
	return
}

func (mc *Connection) Gets(keys ...string) (results []Result, err error) {
	defer handleError(&err)
	results = mc.get("gets", keys)
	return
}

func (mc *Connection) Set(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return mc.store("set", key, flags, timeout, value, 0), nil
}

func (mc *Connection) Add(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return mc.store("add", key, flags, timeout, value, 0), nil
}

func (mc *Connection) Replace(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return mc.store("replace", key, flags, timeout, value, 0), nil
}

func (mc *Connection) Append(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return mc.store("append", key, flags, timeout, value, 0), nil
}

func (mc *Connection) Prepend(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return mc.store("prepend", key, flags, timeout, value, 0), nil
}

func (mc *Connection) Cas(key string, flags uint16, timeout uint64, value []byte, cas uint64) (stored bool, err error) {
	defer handleError(&err)
	return mc.store("cas", key, flags, timeout, value, cas), nil
}

func (mc *Connection) Delete(key string) (deleted bool, err error) {
	defer handleError(&err)
	mc.setDeadline()
	// delete <key> [<time>] [noreply]\r\n
	mc.writestrings("delete ", key, "\r\n")
	reply := mc.readline()
	if strings.Contains(reply, "ERROR") {
		panic(NewMemcacheError("Server error"))
	}
	return strings.HasPrefix(reply, "DELETED"), nil
}

//This purges the entire cache.
func (mc *Connection) FlushAll() (err error) {
	defer handleError(&err)
	mc.setDeadline()
	// flush_all [delay] [noreply]\r\n
	mc.writestrings("flush_all\r\n")
	response := mc.readline()
	if !strings.Contains(response, "OK") {
		panic(NewMemcacheError(fmt.Sprintf("Error in FlushAll %v", response)))
	}
	return nil
}

func (mc *Connection) Stats(argument string) (result []byte, err error) {
	defer handleError(&err)
	mc.setDeadline()
	if argument == "" {
		mc.writestrings("stats\r\n")
	} else {
		mc.writestrings("stats ", argument, "\r\n")
	}
	mc.flush()
	for {
		l := mc.readline()
		if strings.HasPrefix(l, "END") {
			break
		}
		if strings.Contains(l, "ERROR") {
			return nil, NewMemcacheError(l)
		}
		result = append(result, l...)
		result = append(result, '\n')
	}
	return result, err
}

func (mc *Connection) get(command string, keys []string) (results []Result) {
	mc.setDeadline()
	results = make([]Result, 0, len(keys))
	if len(keys) == 0 {
		return
	}
	// get(s) <key>*\r\n
	mc.writestrings(command)
	for _, key := range keys {
		mc.writestrings(" ", key)
	}
	mc.writestrings("\r\n")
	header := mc.readline()
	var result Result
	for strings.HasPrefix(header, "VALUE") {
		// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
		chunks := strings.Split(header, " ")
		if len(chunks) < 4 {
			panic(NewMemcacheError("Malformed response: %s", string(header)))
		}
		result.Key = chunks[1]
		flags64, err := strconv.ParseUint(chunks[2], 10, 16)
		if err != nil {
			panic(NewMemcacheError("%v", err))
		}
		result.Flags = uint16(flags64)
		size, err := strconv.ParseUint(chunks[3], 10, 64)
		if err != nil {
			panic(NewMemcacheError("%v", err))
		}
		if len(chunks) == 5 {
			result.Cas, err = strconv.ParseUint(chunks[4], 10, 64)
			if err != nil {
				panic(NewMemcacheError("%v", err))
			}
		}
		// <data block>\r\n
		result.Value = mc.read(int(size) + 2)[:size]
		results = append(results, result)
		header = mc.readline()
	}
	if !strings.HasPrefix(header, "END") {
		panic(NewMemcacheError("Malformed response: %s", string(header)))
	}
	return
}

func (mc *Connection) store(command, key string, flags uint16, timeout uint64, value []byte, cas uint64) (stored bool) {
	if len(value) > 1000000 {
		return false
	}

	mc.setDeadline()
	// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
	mc.writestrings(command, " ", key, " ")
	mc.write(strconv.AppendUint(nil, uint64(flags), 10))
	mc.writestring(" ")
	mc.write(strconv.AppendUint(nil, timeout, 10))
	mc.writestring(" ")
	mc.write(strconv.AppendInt(nil, int64(len(value)), 10))
	if cas != 0 {
		mc.writestring(" ")
		mc.write(strconv.AppendUint(nil, cas, 10))
	}
	mc.writestring("\r\n")
	// <data block>\r\n
	mc.write(value)
	mc.writestring("\r\n")
	reply := mc.readline()
	if strings.Contains(reply, "ERROR") {
		panic(NewMemcacheError("Server error"))
	}
	return strings.HasPrefix(reply, "STORED")
}

func (mc *Connection) writestrings(strs ...string) {
	for _, s := range strs {
		mc.writestring(s)
	}
}

func (mc *Connection) writestring(s string) {
	if _, err := mc.buffered.WriteString(s); err != nil {
		mc.Close()
		panic(NewMemcacheError("%s", err))
	}
}

func (mc *Connection) write(b []byte) {
	if _, err := mc.buffered.Write(b); err != nil {
		mc.Close()
		panic(NewMemcacheError("%s", err))
	}
}

func (mc *Connection) flush() {
	if err := mc.buffered.Flush(); err != nil {
		mc.Close()
		panic(NewMemcacheError("%s", err))
	}
}

func (mc *Connection) readline() string {
	mc.flush()
	l, isPrefix, err := mc.buffered.ReadLine()
	if isPrefix || err != nil {
		mc.Close()
		panic(NewMemcacheError("Prefix: %v, %s", isPrefix, err))
	}
	return string(l)
}

func (mc *Connection) read(count int) []byte {
	mc.flush()
	b := make([]byte, count)
	if _, err := io.ReadFull(mc.buffered, b); err != nil {
		mc.Close()
		panic(NewMemcacheError("%s", err))
	}
	return b
}

func (mc *Connection) setDeadline() {
	if err := mc.conn.SetDeadline(time.Now().Add(mc.timeout)); err != nil {
		mc.Close()
		panic(NewMemcacheError("%s", err))
	}
}

type MemcacheError struct {
	Message string
}

func NewMemcacheError(format string, args ...interface{}) MemcacheError {
	return MemcacheError{fmt.Sprintf(format, args...)}
}

func (merr MemcacheError) Error() string {
	return merr.Message
}

func handleError(err *error) {
	if x := recover(); x != nil {
		*err = x.(MemcacheError)
	}
}
