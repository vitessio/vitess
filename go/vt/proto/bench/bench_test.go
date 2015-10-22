package bench

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/proto/query"
)

var cellval = []byte("1234567890123456789012345")

func BenchmarkRow1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		vals := make([]byte, 1000, 1000)
		pcells := make([]*query.Cell, 40, 40)
		cells := make([]query.Cell, 40, 40)
		for j := 0; j < 40; j++ {
			copy(vals[j*25:(j+1)*25], cellval)
			cells[j].Value = vals[j*25 : (j+1)*25]
			pcells[j] = &cells[j]
		}
		prow := make([]*query.Row1, 1, 1)
		row := make([]query.Row1, 1, 1)
		row[0].Values = pcells
		prow[0] = &row[0]
		result1 := &query.Result1{Rows: prow}
		process1(result1)
	}
}

func process1(r *query.Result1) *query.Result1 {
	return r
}

func BenchmarkMarshal1(b *testing.B) {
	vals := make([]byte, 1000, 1000)
	pcells := make([]*query.Cell, 40, 40)
	cells := make([]query.Cell, 40, 40)
	for j := 0; j < 40; j++ {
		copy(vals[j*25:(j+1)*25], cellval)
		cells[j].Value = vals[j*25 : (j+1)*25]
		pcells[j] = &cells[j]
	}
	prow := make([]*query.Row1, 1, 1)
	row := make([]query.Row1, 1, 1)
	row[0].Values = pcells
	prow[0] = &row[0]
	result1 := &query.Result1{Rows: prow}
	for i := 0; i < b.N; i++ {
		proto.Marshal(result1)
	}
}

func BenchmarkUnmarshal1(b *testing.B) {
	vals := make([]byte, 1000, 1000)
	pcells := make([]*query.Cell, 40, 40)
	cells := make([]query.Cell, 40, 40)
	for j := 0; j < 40; j++ {
		copy(vals[j*25:(j+1)*25], cellval)
		cells[j].Value = vals[j*25 : (j+1)*25]
		pcells[j] = &cells[j]
	}
	prow := make([]*query.Row1, 1, 1)
	row := make([]query.Row1, 1, 1)
	row[0].Values = pcells
	prow[0] = &row[0]
	result1 := &query.Result1{Rows: prow}
	bytes, err := proto.Marshal(result1)
	if err != nil {
		b.Error(err)
		return
	}
	for i := 0; i < b.N; i++ {
		res := &query.Result1{}
		err = proto.Unmarshal(bytes, res)
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkRow2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		vals := make([]byte, 1000, 1000)
		nulls := make([]bool, 40)
		cells := make([][]byte, 40, 40)
		for j := 0; j < 40; j++ {
			copy(vals[j*25:(j+1)*25], cellval)
			cells[j] = vals[j*25 : (j+1)*25]
			nulls[j] = true
		}
		prow := make([]*query.Row2, 1, 1)
		row := make([]query.Row2, 1, 1)
		row[0].IsNull = nulls
		row[0].Values = cells
		prow[0] = &row[0]
		result2 := &query.Result2{Rows: prow}
		process2(result2)
	}
}

func process2(r *query.Result2) *query.Result2 {
	return r
}

func BenchmarkMarshal2(b *testing.B) {
	vals := make([]byte, 1000, 1000)
	nulls := make([]bool, 40)
	cells := make([][]byte, 40, 40)
	for j := 0; j < 40; j++ {
		copy(vals[j*25:(j+1)*25], cellval)
		cells[j] = vals[j*25 : (j+1)*25]
		nulls[j] = true
	}
	prow := make([]*query.Row2, 1, 1)
	row := make([]query.Row2, 1, 1)
	row[0].IsNull = nulls
	row[0].Values = cells
	prow[0] = &row[0]
	result2 := &query.Result2{Rows: prow}
	for i := 0; i < b.N; i++ {
		proto.Marshal(result2)
	}
}

func BenchmarkUnmarshal2(b *testing.B) {
	vals := make([]byte, 1000, 1000)
	nulls := make([]bool, 40)
	cells := make([][]byte, 40, 40)
	for j := 0; j < 40; j++ {
		copy(vals[j*25:(j+1)*25], cellval)
		cells[j] = vals[j*25 : (j+1)*25]
		nulls[j] = true
	}
	prow := make([]*query.Row2, 1, 1)
	row := make([]query.Row2, 1, 1)
	row[0].IsNull = nulls
	row[0].Values = cells
	prow[0] = &row[0]
	result2 := &query.Result2{Rows: prow}
	bytes, err := proto.Marshal(result2)
	if err != nil {
		b.Error(err)
		return
	}
	for i := 0; i < b.N; i++ {
		res := &query.Result2{}
		err = proto.Unmarshal(bytes, res)
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkRow3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		vals := make([]byte, 0, 1000)
		indexes := make([]int64, 40)
		for j := 0; j < 40; j++ {
			vals = append(vals, cellval...)
			indexes[j] = 25
		}
		prow := make([]*query.Row3, 1, 1)
		row := make([]query.Row3, 1, 1)
		row[0].Indexes = indexes
		row[0].Values = vals
		prow[0] = &row[0]
		result3 := &query.Result3{Rows: prow}
		process3(result3)
	}
}

func process3(r *query.Result3) *query.Result3 {
	return r
}

func BenchmarkMarshal3(b *testing.B) {
	vals := make([]byte, 0, 1000)
	indexes := make([]int64, 40)
	for j := 0; j < 40; j++ {
		vals = append(vals, cellval...)
		indexes[j] = 25
	}
	prow := make([]*query.Row3, 1, 1)
	row := make([]query.Row3, 1, 1)
	row[0].Indexes = indexes
	row[0].Values = vals
	prow[0] = &row[0]
	result3 := &query.Result3{Rows: prow}
	for i := 0; i < b.N; i++ {
		proto.Marshal(result3)
	}
}

func BenchmarkUnmarshal3(b *testing.B) {
	vals := make([]byte, 0, 1000)
	indexes := make([]int64, 40)
	for j := 0; j < 40; j++ {
		vals = append(vals, cellval...)
		indexes[j] = 25
	}
	prow := make([]*query.Row3, 1, 1)
	row := make([]query.Row3, 1, 1)
	row[0].Indexes = indexes
	row[0].Values = vals
	prow[0] = &row[0]
	result3 := &query.Result3{Rows: prow}
	bytes, err := proto.Marshal(result3)
	if err != nil {
		b.Error(err)
		return
	}
	for i := 0; i < b.N; i++ {
		res := &query.Result3{}
		err = proto.Unmarshal(bytes, res)
		if err != nil {
			b.Error(err)
			return
		}
	}
}
