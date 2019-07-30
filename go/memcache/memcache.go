/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package memcache is a client for memcached.
package memcache

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/cacheservice"
)

// Connection is the connection to a memcache.
type Connection struct {
	conn     net.Conn
	buffered bufio.ReadWriter
	timeout  time.Duration
}

// Connect connects a memcache process.
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

// Close closes the memcache connection.
func (mc *Connection) Close() {
	mc.conn.Close()
}

// Get returns cached data for given keys.
func (mc *Connection) Get(keys ...string) (results []cacheservice.Result, err error) {
	defer handleError(&err)
	results = mc.get("get", keys)
	return
}

// Gets returns cached data for given keys, it is an alternative Get api
// for using with CAS. Gets returns a CAS identifier with the item. If
// the item's CAS value has changed since you Gets'ed it, it will not be stored.
func (mc *Connection) Gets(keys ...string) (results []cacheservice.Result, err error) {
	defer handleError(&err)
	results = mc.get("gets", keys)
	return
}

// Set sets the value with specified cache key.
func (mc *Connection) Set(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return mc.store("set", key, flags, timeout, value, 0), nil
}

// Add stores the value only if it does not already exist.
func (mc *Connection) Add(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return mc.store("add", key, flags, timeout, value, 0), nil
}

// Replace replaces the value, only if the value already exists,
// for the specified cache key.
func (mc *Connection) Replace(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return mc.store("replace", key, flags, timeout, value, 0), nil
}

// Append appends the value after the last bytes in an existing item.
func (mc *Connection) Append(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return mc.store("append", key, flags, timeout, value, 0), nil
}

// Prepend prepends the value before existing value.
func (mc *Connection) Prepend(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return mc.store("prepend", key, flags, timeout, value, 0), nil
}

// Cas stores the value only if no one else has updated the data since you read it last.
func (mc *Connection) Cas(key string, flags uint16, timeout uint64, value []byte, cas uint64) (stored bool, err error) {
	defer handleError(&err)
	return mc.store("cas", key, flags, timeout, value, cas), nil
}

// Delete deletes the value for the specified cache key.
func (mc *Connection) Delete(key string) (deleted bool, err error) {
	defer handleError(&err)
	mc.setDeadline()
	// delete <key> [<time>] [noreply]\r\n
	mc.writestrings("delete ", key, "\r\n")
	reply := mc.readline()
	if strings.Contains(reply, "ERROR") {
		panic(NewError("Server error"))
	}
	return strings.HasPrefix(reply, "DELETED"), nil
}

// FlushAll purges the entire cache.
func (mc *Connection) FlushAll() (err error) {
	defer handleError(&err)
	mc.setDeadline()
	// flush_all [delay] [noreply]\r\n
	mc.writestrings("flush_all\r\n")
	response := mc.readline()
	if !strings.Contains(response, "OK") {
		panic(NewError(fmt.Sprintf("Error in FlushAll %v", response)))
	}
	return nil
}

// Stats returns a list of basic stats.
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
			return nil, NewError(l)
		}
		result = append(result, l...)
		result = append(result, '\n')
	}
	return result, err
}

func (mc *Connection) get(command string, keys []string) (results []cacheservice.Result) {
	mc.setDeadline()
	results = make([]cacheservice.Result, 0, len(keys))
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
	var result cacheservice.Result
	for strings.HasPrefix(header, "VALUE") {
		// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
		chunks := strings.Split(header, " ")
		if len(chunks) < 4 {
			panic(NewError("Malformed response: %s", string(header)))
		}
		result.Key = chunks[1]
		flags64, err := strconv.ParseUint(chunks[2], 10, 16)
		if err != nil {
			panic(NewError("%v", err))
		}
		result.Flags = uint16(flags64)
		size, err := strconv.ParseUint(chunks[3], 10, 64)
		if err != nil {
			panic(NewError("%v", err))
		}
		if len(chunks) == 5 {
			result.Cas, err = strconv.ParseUint(chunks[4], 10, 64)
			if err != nil {
				panic(NewError("%v", err))
			}
		}
		// <data block>\r\n
		result.Value = mc.read(int(size) + 2)[:size]
		results = append(results, result)
		header = mc.readline()
	}
	if !strings.HasPrefix(header, "END") {
		panic(NewError("Malformed response: %s", string(header)))
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
		panic(NewError("Server error"))
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
		panic(NewError("%s", err))
	}
}

func (mc *Connection) write(b []byte) {
	if _, err := mc.buffered.Write(b); err != nil {
		panic(NewError("%s", err))
	}
}

func (mc *Connection) flush() {
	if err := mc.buffered.Flush(); err != nil {
		panic(NewError("%s", err))
	}
}

func (mc *Connection) readline() string {
	mc.flush()
	l, isPrefix, err := mc.buffered.ReadLine()
	if isPrefix || err != nil {
		panic(NewError("Prefix: %v, %s", isPrefix, err))
	}
	return string(l)
}

func (mc *Connection) read(count int) []byte {
	mc.flush()
	b := make([]byte, count)
	if _, err := io.ReadFull(mc.buffered, b); err != nil {
		panic(NewError("%s", err))
	}
	return b
}

func (mc *Connection) setDeadline() {
	if err := mc.conn.SetDeadline(time.Now().Add(mc.timeout)); err != nil {
		panic(NewError("%s", err))
	}
}

// Error is the error from CacheService.
type Error struct {
	Message string
}

// NewError creates a new Error.
func NewError(format string, args ...interface{}) Error {
	return Error{fmt.Sprintf(format, args...)}
}

func (merr Error) Error() string {
	return merr.Message
}

func handleError(err *error) {
	if x := recover(); x != nil {
		*err = x.(Error)
	}
}

func init() {
	cacheservice.Register(
		"memcache",
		func(config cacheservice.Config) (cacheservice.CacheService, error) {
			return Connect(config.Address, config.Timeout)
		},
	)
}
