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
	"os/exec"
	"testing"
	"time"
)

func TestMemcache(t *testing.T) {
	cmd := exec.Command("memcached", "-s", "/tmp/vtocc_cache.sock")
	if err := cmd.Start(); err != nil {
		t.Errorf("Memcache start: %v", err)
		return
	}
	defer cmd.Process.Kill()
	time.Sleep(time.Second)

	c, err := Connect("/tmp/vtocc_cache.sock")
	if err != nil {
		t.Errorf("Connect: %v", err)
		return
	}

	// Set
	stored, err := c.Set("Hello", 0, 0, []byte("world"))
	if err != nil {
		t.Errorf("Set: %v", err)
		return
	}
	if !stored {
		t.Errorf("Expecting true, received %v", stored)
	}
	expect(t, c, "Hello", "world")

	// Add
	stored, err = c.Add("Hello", 0, 0, []byte("Jupiter"))
	if err != nil {
		t.Errorf("Add: %v", err)
	}
	if stored {
		t.Errorf("Expecting false, received %v", stored)
	}
	expect(t, c, "Hello", "world")

	// Replace
	stored, err = c.Replace("Hello", 0, 0, []byte("World"))
	if err != nil {
		t.Errorf("Replace: %v", err)
	}
	if !stored {
		t.Errorf("Expecting true, received %v", stored)
	}
	expect(t, c, "Hello", "World")

	// Append
	stored, err = c.Append("Hello", 0, 0, []byte("!"))
	if err != nil {
		t.Errorf("Append: %v", err)
	}
	if !stored {
		t.Errorf("Expecting true, received %v", stored)
	}
	expect(t, c, "Hello", "World!")

	// Prepend
	stored, err = c.Prepend("Hello", 0, 0, []byte("Hello, "))
	if err != nil {
		t.Errorf("Prepend: %v", err)
	}
	if !stored {
		t.Errorf("Expecting true, received %v", stored)
	}
	expect(t, c, "Hello", "Hello, World!")

	// Delete
	deleted, err := c.Delete("Hello")
	if err != nil {
		t.Errorf("Delete: %v", err)
	}
	if !deleted {
		t.Errorf("Expecting true, received %v", deleted)
	}
	expect(t, c, "Hello", "")

	// Flags
	stored, err = c.Set("Hello", 0xFFFF, 0, []byte("world"))
	if err != nil {
		t.Errorf("Set: %v", err)
		return
	}
	if !stored {
		t.Errorf("Expecting true, received %v", stored)
	}
	b, f, err := c.Get("Hello")
	if err != nil {
		t.Errorf("Get: %v", err)
		return
	}
	if f != 0xFFFF {
		t.Errorf("Expecting 0xFFFF, Received %x", f)
	}
	if string(b) != "world" {
		t.Errorf("Expecting world, Received %s", b)
	}

	// timeout
	stored, err = c.Set("Lost", 0, 1, []byte("World"))
	if err != nil {
		t.Errorf("Set: %v", err)
		return
	}
	if !stored {
		t.Errorf("Expecting true, received %v", stored)
	}
	expect(t, c, "Lost", "World")
	time.Sleep(2 * time.Second)
	expect(t, c, "Lost", "")
}

func expect(t *testing.T, c *Connection, key, value string) {
	b, _, err := c.Get(key)
	if err != nil {
		t.Errorf("Get: %v", err)
		return
	}
	if string(b) != value {
		t.Errorf("Expecting %s, Received %s", value, b)
	}
}
