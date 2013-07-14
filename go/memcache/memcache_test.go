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
		t.Errorf("want true, got %v", stored)
	}
	expect(t, c, "Hello", "world")

	// Add
	stored, err = c.Add("Hello", 0, 0, []byte("Jupiter"))
	if err != nil {
		t.Errorf("Add: %v", err)
	}
	if stored {
		t.Errorf("want false, got %v", stored)
	}
	expect(t, c, "Hello", "world")

	// Replace
	stored, err = c.Replace("Hello", 0, 0, []byte("World"))
	if err != nil {
		t.Errorf("Replace: %v", err)
	}
	if !stored {
		t.Errorf("want true, got %v", stored)
	}
	expect(t, c, "Hello", "World")

	// Append
	stored, err = c.Append("Hello", 0, 0, []byte("!"))
	if err != nil {
		t.Errorf("Append: %v", err)
	}
	if !stored {
		t.Errorf("want true, got %v", stored)
	}
	expect(t, c, "Hello", "World!")

	// Prepend
	stored, err = c.Prepend("Hello", 0, 0, []byte("Hello, "))
	if err != nil {
		t.Errorf("Prepend: %v", err)
	}
	if !stored {
		t.Errorf("want true, got %v", stored)
	}
	expect(t, c, "Hello", "Hello, World!")

	// Delete
	deleted, err := c.Delete("Hello")
	if err != nil {
		t.Errorf("Delete: %v", err)
	}
	if !deleted {
		t.Errorf("want true, got %v", deleted)
	}
	expect(t, c, "Hello", "")

	// Flags
	stored, err = c.Set("Hello", 0xFFFF, 0, []byte("world"))
	if err != nil {
		t.Errorf("Set: %v", err)
		return
	}
	if !stored {
		t.Errorf("want true, got %v", stored)
	}
	results, err := c.Get("Hello")
	if err != nil {
		t.Errorf("Get: %v", err)
		return
	}
	if results[0].Flags != 0xFFFF {
		t.Errorf("want 0xFFFF, got %x", results[0].Flags)
	}
	if string(results[0].Value) != "world" {
		t.Errorf("want world, got %s", results[0].Value)
	}

	// timeout
	stored, err = c.Set("Lost", 0, 1, []byte("World"))
	if err != nil {
		t.Errorf("Set: %v", err)
		return
	}
	if !stored {
		t.Errorf("want true, got %v", stored)
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
		t.Errorf("want true, got %v", stored)
	}
	expect(t, c, "Data", "Set")
	results, err = c.Gets("Data")
	if err != nil {
		t.Errorf("Gets: %v", err)
		return
	}
	cas := results[0].Cas
	if cas == 0 {
		t.Errorf("want non-zero for cas")
	}
	stored, err = c.Cas("Data", 0, 0, []byte("not set"), 12345)
	if err != nil {
		t.Errorf("Set: %v", err)
		return
	}
	if stored {
		t.Errorf("want false, got %v", stored)
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
		t.Errorf("want true, got %v", stored)
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

	// FlushAll
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

	results, err = c.Get("Flush")
	if err != nil {
		t.Errorf("Get: %v", err)
		return
	}
	if len(results) != 0 {
		t.Errorf("FlushAll failed")
		return
	}

	// Multi
	stored, _ = c.Set("key1", 0, 0, []byte("val1"))
	stored, _ = c.Set("key2", 0, 0, []byte("val2"))

	results, _ = c.Get("key1", "key2")
	if len(results) != 2 {
		t.Fatalf("want 2, gto %d", len(results))
	}
	if results[0].Key != "key1" {
		t.Errorf("want key1, got %s", results[0].Key)
	}
	if string(results[0].Value) != "val1" {
		t.Errorf("want val1, got %s", string(results[0].Value))
	}
	if results[1].Key != "key2" {
		t.Errorf("want key2, got %s", results[0].Key)
	}
	if string(results[1].Value) != "val2" {
		t.Errorf("want val2, got %s", string(results[1].Value))
	}

	results, _ = c.Gets("key1", "key3", "key2")
	if len(results) != 2 {
		t.Fatalf("want 2, gto %d", len(results))
	}
	if results[0].Key != "key1" {
		t.Errorf("want key1, got %s", results[0].Key)
	}
	if string(results[0].Value) != "val1" {
		t.Errorf("want val1, got %s", string(results[0].Value))
	}
	if results[1].Key != "key2" {
		t.Errorf("want key2, got %s", results[0].Key)
	}
	if string(results[1].Value) != "val2" {
		t.Errorf("want val2, got %s", string(results[1].Value))
	}
}

func expect(t *testing.T, c *Connection, key, value string) {
	results, err := c.Get(key)
	if err != nil {
		t.Errorf("Get: %v", err)
		return
	}
	var got string
	if len(results) != 0 {
		got = string(results[0].Value)
	}
	if got != value {
		t.Errorf("want %s, got %s", value, results[0].Value)
	}
}
