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

// Package hack gives you some efficient functionality at the cost 
// breaking some go rules

package hack

import (
	"reflect"
	"unsafe"
)

// StringArena lets you consolidate allocations for a group of strings
// that have similar life length
type StringArena struct {
	buf []byte
	str string
}

// NewStringArena creates an arena of the specified size.
func NewStringArena(size int) *StringArena {
	self := &StringArena{buf: make([]byte, 0, size)}
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&self.buf))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&self.str))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Cap
	return self
}

// NewString copies a byte slice into the arena and returns it as a string.
// If the arena is full, it returns a traditional go string.
func (self *StringArena) NewString(b []byte) string {
	if len(self.buf)+len(b) > cap(self.buf) {
		return string(b)
	}
	start := len(self.buf)
	self.buf = append(self.buf, b...)
	return self.str[start : start+len(b)]
}

// SpaceLeft returns the amount of space left in the arena.
func (self *StringArena) SpaceLeft() int {
	return cap(self.buf) - len(self.buf)
}
