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

package memcache

import (
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestMemcache(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}

	cmd := exec.Command("memcached", "-s", "/tmp/vt_memcache_test.sock")
	if err := cmd.Start(); err != nil {
		if strings.Contains(err.Error(), "executable file not found in $PATH") {
			t.Skipf("skipping: %v", err)
		}
		t.Fatalf("Memcache start: %v", err)
	}
	defer cmd.Process.Kill()

	var c *Connection
	var err error
	deadline := time.Now().Add(10 * time.Second)
	for {
		if c, err = Connect("/tmp/vt_memcache_test.sock", 30*time.Millisecond); err == nil {
			break
		} else {
			if time.Now().After(deadline) {
				t.Fatalf("Connect: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Set
	stored, err := c.Set("Hello", 0, 0, []byte("world"))
	if err != nil {
		t.Fatalf("Set: %v", err)
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
		t.Fatalf("Set: %v", err)
	}
	if !stored {
		t.Errorf("want true, got %v", stored)
	}
	results, err := c.Get("Hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
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
		t.Fatalf("Set: %v", err)
	}
	if !stored {
		t.Errorf("want true, got %v", stored)
	}
	expect(t, c, "Lost", "World")
	expectPoll(t, c, "Lost", "", 10*time.Second)

	// cas
	stored, err = c.Set("Data", 0, 0, []byte("Set"))
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !stored {
		t.Errorf("want true, got %v", stored)
	}
	expect(t, c, "Data", "Set")
	results, err = c.Gets("Data")
	if err != nil {
		t.Fatalf("Gets: %v", err)
	}
	cas := results[0].Cas
	if cas == 0 {
		t.Errorf("want non-zero for cas")
	}
	stored, err = c.Cas("Data", 0, 0, []byte("not set"), 12345)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if stored {
		t.Errorf("want false, got %v", stored)
	}
	expect(t, c, "Data", "Set")
	_, err = c.Cas("Data", 0, 0, []byte("Changed"), cas)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	expect(t, c, "Data", "Changed")
	stored, err = c.Set("Data", 0, 0, []byte("Overwritten"))
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !stored {
		t.Errorf("want true, got %v", stored)
	}
	expect(t, c, "Data", "Overwritten")

	// stats
	var stats []byte
	stats, err = c.Stats("")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	statsStr := string(stats)
	if !strings.Contains(statsStr, "STAT version ") {
		t.Fatalf("want containing \"version\", got %v", statsStr)
	}
	// for manual inspection of stats with -v
	t.Logf("Main stats:\n" + statsStr)

	stats, err = c.Stats("slabs")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	statsStr = string(stats)
	if !strings.Contains(statsStr, "STAT 1:chunk_size ") {
		t.Fatalf("want containing \"chunk_size\", got %v", statsStr)
	}
	// for manual inspection of stats with -v
	t.Logf("Slabs stats:\n" + string(stats))

	stats, err = c.Stats("items")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	statsStr = string(stats)
	if !strings.Contains(statsStr, "STAT items:1:number ") {
		t.Fatalf("want containing \"number\", got %v", statsStr)
	}
	// for manual inspection of stats with -v
	t.Logf("Items stats:\n" + string(stats))

	// FlushAll
	// Set
	_, err = c.Set("Flush", 0, 0, []byte("Test"))
	if err != nil {
		t.Errorf("Set: %v", err)
	}
	expect(t, c, "Flush", "Test")

	err = c.FlushAll()
	if err != nil {
		t.Fatalf("FlushAll: err %v", err)
	}

	results, err = c.Get("Flush")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("FlushAll failed")
	}

	// Multi
	_, _ = c.Set("key1", 0, 0, []byte("val1"))
	_, _ = c.Set("key2", 0, 0, []byte("val2"))

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

	// timeout test
	c.timeout = 1 * time.Nanosecond
	_, err = c.Gets("key1", "key3", "key2")
	want := "i/o timeout"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("\"%v\" does not contain \"%s\"", err, want)
	}

	// test double close
	c.Close()
	c.Close()
}

func expect(t *testing.T, c *Connection, key, value string) {
	results, err := c.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var got string
	if len(results) != 0 {
		got = string(results[0].Value)
	}
	if got != value {
		t.Errorf("want %s, got %s", value, got)
	}
}

func expectPoll(t *testing.T, c *Connection, key, value string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		results, err := c.Get(key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		var got string
		if len(results) != 0 {
			got = string(results[0].Value)
		}
		if got == value {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("want %s, got %s", value, got)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
