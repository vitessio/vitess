// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"bytes"
	"fmt"
	"sync"
)

// Matrix provides two-dimension map from string.string to int64.
// It also provides a Counts method which can be used for dumping data.
type Matrix struct {
	mu   sync.Mutex
	data map[string]map[string]int64
}

func NewMatrix(name string) *Matrix {
	m := &Matrix{data: make(map[string]map[string]int64)}
	if name != "" {
		Publish(name, m)
	}
	return m
}

func (m *Matrix) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return matrixToString(m.data)
}

func (m *Matrix) Add(name1, name2 string, value int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mat, ok := m.data[name1]
	if !ok {
		mat = make(map[string]int64)
		m.data[name1] = mat
	}
	mat[name2] += value
}

func (m *Matrix) Counts() map[string]map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	data := make(map[string]map[string]int64, len(m.data))
	for k1, v1 := range m.data {
		temp := make(map[string]int64, len(v1))
		for k2, v2 := range v1 {
			temp[k2] = v2
		}
		data[k1] = temp
	}
	return data
}

// MatrixFunc converts a function that returns
// a two dimension map of int64 as an expvar.
type MatrixFunc func() map[string]map[string]int64

func (f MatrixFunc) String() string {
	m := f()
	if m == nil {
		return "{}"
	}
	return matrixToString(m)
}

func matrixToString(m map[string]map[string]int64) string {
	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")
	firstValue := true
	for k1, v1 := range m {
		for k2, v2 := range v1 {
			if firstValue {
				firstValue = false
			} else {
				fmt.Fprintf(b, ", ")
			}
			fmt.Fprintf(b, "\"%v.%v\": %v", k1, k2, v2)
		}
	}
	fmt.Fprintf(b, "}")
	return b.String()
}
