// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

	// cas
	stored, err = c.Set("Data", 0, 0, []byte("Set"))
	if err != nil {
		t.Errorf("Set: %v", err)
		return
	}
	if !stored {
		t.Errorf("Expecting true, received %v", stored)
	}
	expect(t, c, "Data", "Set")
	b, f, cas, err := c.Gets("Data")
	if err != nil {
		t.Errorf("Gets: %v", err)
		return
	}
	if cas == 0 {
		t.Errorf("Expecting non-zero for cas")
	}
	stored, err = c.Cas("Data", 0, 0, []byte("not set"), 12345)
	if err != nil {
		t.Errorf("Set: %v", err)
		return
	}
	if stored {
		t.Errorf("Expecting false, received %v", stored)
	}
	expect(t, c, "Data", "Set")
	stored, err = c.Cas("Data", 0, 0, []byte("Changed"), cas)
	if err != nil {
		t.Errorf("Set: %v", err)
		return
	}
	expect(t, c, "Data", "Changed")
	stored, err = c.Set("Data", 0, 0, []byte("Overwritten"))
	if err != nil {
		t.Errorf("Set: %v", err)
		return
	}
	if !stored {
		t.Errorf("Expecting true, received %v", stored)
	}
	expect(t, c, "Data", "Overwritten")

	// stats
	_, err = c.Stats("")
	if err != nil {
		t.Errorf("Stats: %v", err)
		return
	}

	_, err = c.Stats("slabs")
	if err != nil {
		t.Errorf("Stats: %v", err)
		return
	}

	//FlushAll
	// Set
	stored, err = c.Set("Flush", 0, 0, []byte("Test"))
	if err != nil {
		t.Errorf("Set: %v", err)
	}
	expect(t, c, "Flush", "Test")

	err = c.FlushAll()
	if err != nil {
		t.Errorf("FlushAll: err %v", err)
		return
	}

	b, f, err = c.Get("Flush")
	if err != nil {
		t.Errorf("Get: %v", err)
		return
	}
	if string(b) != "" {
		t.Errorf("FlushAll failed")
		return
	}
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
