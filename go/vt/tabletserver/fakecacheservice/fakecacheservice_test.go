// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakecacheservice

import (
	"reflect"
	"testing"

	cs "github.com/youtube/vitess/go/cacheservice"
)

func TestRegisterFakeCacheService(t *testing.T) {
	Register()
	service, err := cs.Connect(cs.Config{})
	if err != nil {
		t.Fatalf("got error when creating a new fake cacheservice: %v", err)
	}
	if s, ok := service.(*FakeCacheService); !ok {
		t.Fatalf("created service is not a fake cacheservice, cacheservice: %v", s)
	}
}

func TestFakeCacheService(t *testing.T) {
	service := NewFakeCacheService(&Cache{data: make(map[string]*cs.Result)})
	key1 := "key1"
	key2 := "key2"
	keys := []string{key1, key2}
	results, err := service.Get(keys...)
	if err != nil {
		t.Fatalf("get error: %v", err)
	}
	if !reflect.DeepEqual(results, []cs.Result{}) {
		t.Fatalf("get should return empty results, but get: %v", results)
	}
	// test Set then Get
	service.Set(key1, 0, 0, []byte("test"))
	results, err = service.Get(key1)
	if !reflect.DeepEqual(results[0].Value, []byte("test")) {
		t.Fatalf("expect to get value: test, but get: %s", string(results[0].Value))
	}
	// test Gets and Cas
	results, err = service.Gets(key1)
	if results[0].Cas == 0 {
		t.Fatalf("cas should be set")
	}
	stored, err := service.Cas(key1, 0, 0, []byte("test2"), 0)
	if stored {
		t.Fatalf("cas operation should fail")
	}
	stored, err = service.Cas(key1, 0, 0, []byte("test2"), results[0].Cas)
	if !stored {
		t.Fatalf("cas operation should succeed")
	}
	// test Add
	stored, err = service.Add(key1, 0, 0, []byte("test3"))
	if stored {
		t.Fatalf("key already exists, Add should fail")
	}
	stored, err = service.Add(key2, 0, 0, []byte("test3"))
	if !stored {
		t.Fatalf("key does not exist and Add should succeed")
	}
	// test Replace
	stored, err = service.Replace("unknownKey", 0, 0, []byte("test4"))
	if stored {
		t.Fatalf("key does not exist, Replace should fail")
	}
	service.Set(key2, 0, 0, []byte("test3"))
	stored, err = service.Replace(key2, 0, 0, []byte("test4"))
	results, err = service.Get(key2)
	if !stored || !reflect.DeepEqual(results[0].Value, []byte("test4")) {
		t.Fatalf("key already exists, Replace should succeed, expect to get: %s, but got: %s", "test4", string(results[0].Value))
	}
	// test Append
	stored, err = service.Append("unknownKey", 0, 0, []byte("test5"))
	if stored {
		t.Fatalf("key does not exist, Append should fail")
	}
	service.Set(key2, 0, 0, []byte("test4"))
	stored, err = service.Append(key2, 0, 0, []byte("test5"))
	results, err = service.Get(key2)
	if !stored || !reflect.DeepEqual(results[0].Value, []byte("test4test5")) {
		t.Fatalf("key already exists, Append should succeed")
	}
	// test Prepend
	stored, err = service.Prepend("unknownKey", 0, 0, []byte("test5"))
	if stored {
		t.Fatalf("key does not exist, Prepend should fail")
	}
	service.Set(key2, 0, 0, []byte("test4"))
	stored, err = service.Prepend(key2, 0, 0, []byte("test5"))
	results, err = service.Get(key2)
	if !stored || !reflect.DeepEqual(results[0].Value, []byte("test5test4")) {
		t.Fatalf("key already exists, Prepend should succeed")
	}
	// test Delete
	service.Set(key2, 0, 0, []byte("aaa"))
	results, err = service.Get(key2)
	if !reflect.DeepEqual(results[0].Value, []byte("aaa")) {
		t.Fatalf("set key does not succeed")
	}
	if ok, _ := service.Delete(key2); !ok {
		t.Fatalf("delete should succeed")
	}
	results, err = service.Get(key2)
	if !reflect.DeepEqual(results, []cs.Result{}) {
		t.Fatalf("key does not exists, should get empty result, but got: %v", results)
	}
	// test FlushAll
	service.Set(key1, 0, 0, []byte("aaa"))
	service.Set(key2, 0, 0, []byte("bbb"))
	err = service.FlushAll()
	if err != nil {
		t.Fatalf("FlushAll failed")
	}
	results, err = service.Get(key1, key2)
	if !reflect.DeepEqual(results, []cs.Result{}) {
		t.Fatalf("cache has been flushed, should only get empty results")
	}
	service.Stats("")
	service.Close()
}

func TestFakeCacheServiceError(t *testing.T) {
	service := NewFakeCacheService(&Cache{data: make(map[string]*cs.Result)})
	service.cache.EnableCacheServiceError()
	key1 := "key1"
	_, err := service.Set(key1, 0, 0, []byte("test"))
	checkCacheServiceError(t, err)
	_, err = service.Get(key1)
	checkCacheServiceError(t, err)
	_, err = service.Gets(key1)
	checkCacheServiceError(t, err)
	_, err = service.Cas(key1, 0, 0, []byte("test2"), 0)
	checkCacheServiceError(t, err)
	_, err = service.Add(key1, 0, 0, []byte("test3"))
	checkCacheServiceError(t, err)
	_, err = service.Replace("unknownKey", 0, 0, []byte("test4"))
	checkCacheServiceError(t, err)
	_, err = service.Append("unknownKey", 0, 0, []byte("test5"))
	checkCacheServiceError(t, err)
	_, err = service.Prepend("unknownKey", 0, 0, []byte("test5"))
	checkCacheServiceError(t, err)
	_, err = service.Prepend(key1, 0, 0, []byte("test5"))
	checkCacheServiceError(t, err)
	_, err = service.Delete(key1)
	checkCacheServiceError(t, err)
	err = service.FlushAll()
	checkCacheServiceError(t, err)
	_, err = service.Stats("")
	checkCacheServiceError(t, err)

	service.cache.DisableCacheServiceError()
	ok, err := service.Set(key1, 0, 0, []byte("test"))
	if !ok || err != nil {
		t.Fatalf("set should succeed")
	}
	results, err := service.Get(key1)
	if !reflect.DeepEqual(results[0].Value, []byte("test")) {
		t.Fatalf("expect to get value: test, but get: %s", string(results[0].Value))
	}
	service.Close()
}

func checkCacheServiceError(t *testing.T, err error) {
	if err.Error() != errCacheService {
		t.Fatalf("should get cacheservice error")
	}
}
