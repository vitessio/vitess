// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"bytes"
	"fmt"
	"sync"
)

// MatrixType provides a common interface for Matrix and MatrixFunc.
type MatrixType interface {
	LabelX() string
	LabelY() string
	Data() map[string]map[string]int64
}

// Matrix provides a two-dimentional map from string.string to int64.
// It also provides a Data method which can be used for dumping data.
type Matrix struct {
	mu     sync.Mutex
	labelX string
	labelY string
	data   map[string]map[string]int64
}

func NewMatrix(name, labelX, labelY string) *Matrix {
	m := &Matrix{
		labelX: labelX,
		labelY: labelY,
		data:   make(map[string]map[string]int64),
	}
	if name != "" {
		Publish(name, m)
	}
	return m
}

func (m *Matrix) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return matrixToString(m.labelX, m.labelY, m.data)
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

func (m *Matrix) LabelX() string {
	return m.labelX
}

func (m *Matrix) LabelY() string {
	return m.labelY
}

func (m *Matrix) Data() map[string]map[string]int64 {
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
// a two-dimentional map of int64 as an expvar.
type MatrixFunc struct {
	labelX string
	labelY string
	f      func() map[string]map[string]int64
}

func NewMatrixFunc(labelX, labelY string, f func() map[string]map[string]int64) *MatrixFunc {
	m := &MatrixFunc{labelX: labelX, labelY: labelY, f: f}
	return m
}

func (m *MatrixFunc) LabelX() string {
	return m.labelX
}

func (m *MatrixFunc) LabelY() string {
	return m.labelY
}

func (m *MatrixFunc) Data() map[string]map[string]int64 {
	return m.f()
}

func (m *MatrixFunc) String() string {
	data := m.f()
	if data == nil {
		return "{}"
	}
	return matrixToString(m.labelX, m.labelY, data)
}

func matrixToString(labelX, labelY string, m map[string]map[string]int64) string {
	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "[")
	firstValue := true
	for k1, v1 := range m {
		for k2, v2 := range v1 {
			if firstValue {
				firstValue = false
			} else {
				fmt.Fprintf(b, ", ")
			}
			fmt.Fprintf(b, `{"%v": "%v", "%v": "%v", "Value": %v}`, labelX, k1, labelY, k2, v2)
		}
	}
	fmt.Fprintf(b, "]")
	return b.String()
}
