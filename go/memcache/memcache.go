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

package memcache

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Connection struct {
	conn     net.Conn
	buffered bufio.ReadWriter
}

func Connect(address string) (conn *Connection, err error) {
	var network string
	if strings.Contains(address, "/") {
		network = "unix"
	} else {
		network = "tcp"
	}
	var nc net.Conn
	nc, err = net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return newConnection(nc), nil
}

func newConnection(nc net.Conn) *Connection {
	return &Connection{
		conn: nc,
		buffered: bufio.ReadWriter{
			bufio.NewReader(nc),
			bufio.NewWriter(nc),
		},
	}
}

func (self *Connection) Close() {
	self.conn.Close()
	self.conn = nil
}

func (self *Connection) IsClosed() bool {
	return self.conn == nil
}

func (self *Connection) Get(key string) (value []byte, flags uint16, err error) {
	defer handleError(&err)
	// get <key>*\r\n
	self.writestrings("get ", key, "\r\n")
	header := self.readline()
	if strings.HasPrefix(header, "VALUE") {
		// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
		chunks := strings.Split(header, " ")
		if len(chunks) < 4 {
			panic(NewMemcacheError("Malformed response: %s", string(header)))
		}
		flags64, err := strconv.ParseUint(chunks[2], 10, 16)
		if err != nil {
			panic(NewMemcacheError("%v", err))
		}
		flags = uint16(flags64)
		size, err := strconv.ParseUint(chunks[3], 10, 64)
		if err != nil {
			panic(NewMemcacheError("%v", err))
		}
		// <data block>\r\n
		value = self.read(int(size) + 2)[:size]
		header = self.readline()
	}
	if !strings.HasPrefix(header, "END") {
		panic(NewMemcacheError("Malformed response: %s", string(header)))
	}
	return value, flags, nil
}

func (self *Connection) Set(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return self.store("set", key, flags, timeout, value), nil
}

func (self *Connection) Add(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return self.store("add", key, flags, timeout, value), nil
}

func (self *Connection) Replace(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return self.store("replace", key, flags, timeout, value), nil
}

func (self *Connection) Append(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return self.store("append", key, flags, timeout, value), nil
}

func (self *Connection) Prepend(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return self.store("prepend", key, flags, timeout, value), nil
}

func (self *Connection) Delete(key string) (deleted bool, err error) {
	defer handleError(&err)
	// delete <key> [<time>] [noreply]\r\n
	self.writestrings("delete ", key, "\r\n")
	reply := self.readline()
	if strings.Contains(reply, "ERROR") {
		panic(NewMemcacheError("Server error"))
	}
	return strings.HasPrefix(reply, "DELETED"), nil
}

func (self *Connection) store(command, key string, flags uint16, timeout uint64, value []byte) (stored bool) {
	if len(value) > 1000000 {
		return false
	}

	// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
	self.writestrings(command, " ", key, " ")
	self.write(strconv.AppendUint(nil, uint64(flags), 10))
	self.writestring(" ")
	self.write(strconv.AppendUint(nil, timeout, 10))
	self.writestring(" ")
	self.write(strconv.AppendInt(nil, int64(len(value)), 10))
	self.writestring("\r\n")
	// <data block>\r\n
	self.write(value)
	self.writestring("\r\n")
	reply := self.readline()
	if strings.Contains(reply, "ERROR") {
		panic(NewMemcacheError("Server error"))
	}
	return strings.HasPrefix(reply, "STORED")
}

func (self *Connection) writestrings(strs ...string) {
	for _, s := range strs {
		self.writestring(s)
	}
}

func (self *Connection) writestring(s string) {
	if _, err := self.buffered.WriteString(s); err != nil {
		panic(NewMemcacheError("%s", err))
	}
}

func (self *Connection) write(b []byte) {
	if _, err := self.buffered.Write(b); err != nil {
		panic(NewMemcacheError("%s", err))
	}
}

func (self *Connection) flush() {
	if err := self.buffered.Flush(); err != nil {
		panic(NewMemcacheError("%s", err))
	}
}

func (self *Connection) readline() string {
	self.flush()
	l, isPrefix, err := self.buffered.ReadLine()
	if isPrefix || err != nil {
		panic(NewMemcacheError("Prefix: %v, %s", isPrefix, err))
	}
	return string(l)
}

func (self *Connection) read(count int) []byte {
	self.flush()
	b := make([]byte, count)
	total := 0
	for i := 0; i < 10; i++ {
		n, err := self.buffered.Read(b[total:])
		if err != nil {
			panic(NewMemcacheError("%s", err))
		}
		total += n
		if total >= count {
			break
		}
	}
	if total != count {
		panic(NewMemcacheError("Unifinished read"))
	}
	return b
}

type MemcacheError struct {
	Message string
}

func NewMemcacheError(format string, args ...interface{}) MemcacheError {
	return MemcacheError{fmt.Sprintf(format, args...)}
}

func (self MemcacheError) Error() string {
	return self.Message
}

func handleError(err *error) {
	if x := recover(); x != nil {
		*err = x.(MemcacheError)
	}
}
