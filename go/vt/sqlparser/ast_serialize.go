package sqlparser

import (
	"bytes"
	"fmt"
)

func Serialize(in Statement) ([]byte, error) {
	s := serializer{}

	s.serialize(in)
	return s.buf.Bytes(), nil
}

const (
	// statement implementations
	tSelect byte = iota
)
const (
	// SelectExpr implementations
	tAliasedExpr byte = iota
)
const (
	// Expr implementations
	tLiteral byte = iota
)
const (
	// TableExpr implementations
	tAliasedTableExpr byte = iota
)
const (
	// SimpleTableExpr implementations
	tTableName byte = iota
)

type serializer struct {
	buf bytes.Buffer
}

func encodeNils(buf []bool, startIdx int, refs ...interface{}) {
	for i, f := range refs {
		if isNilValue(f) {
			buf[startIdx+i] = true
		}
	}
}

func (s *serializer) serialize(in SQLNode) {
	switch node := in.(type) {
	case *Literal:
		s.putByte(tLiteral)
		s.putByte(byte(node.Type))
		s.putByteSlice(node.Val)
	case *Select:
		s.putByte(tSelect)
		s.putRefBool(node.Cache)
		boolsAndNils := make([]bool, 11)
		if node.Distinct {
			boolsAndNils[0] = true
		}
		if node.SQLCalcFoundRows {
			boolsAndNils[1] = true
		}
		if node.StraightJoinHint {
			boolsAndNils[2] = true
		}
		encodeNils(boolsAndNils, 3, node.Comments, node.SelectExprs, node.From, node.Where, node.GroupBy, node.Having, node.OrderBy, node.Limit)
		s.putBools(boolsAndNils...)
		if node.SelectExprs != nil {
			s.serialize(node.SelectExprs)
		}
		if node.From != nil {
			s.serialize(node.From)
		}
	case SelectExprs:
		s.putVInt(len(node)) // size of slice
		for _, e := range node {
			s.serialize(e)
		}
	case *AliasedExpr:
		s.putByte(tAliasedExpr) // type
		s.serialize(node.As)
		s.serialize(node.Expr)
	case ColIdent:
		s.putString(node.val)
	case TableExprs:
		s.putVInt(len(node)) // size of slice
		for _, e := range node {
			s.serialize(e)
		}
	case *AliasedTableExpr:
		s.putByte(tAliasedTableExpr)
		s.serialize(node.Expr)
	case TableName:
		s.putByte(tTableName)
		s.putString(node.Name.v)

	default:
		panic(fmt.Sprintf("unknown type: %T", node))
	}
}

func (s *serializer) encodeNullableFields(fields ...interface{}) {
	bools := make([]bool, len(fields))
	for i, f := range fields {
		if isNilValue(f) {
			bools[i] = true
		}
	}
	s.putBools(bools...)
}

func (s *serializer) putBools(p ...bool) {
	if len(p) > 8 {
		s.putByte(0xfc)
		s.putBools(p[:7]...)
		s.putBools(p[8:]...)
		return
	}
	var res byte
	for i, b := range p {
		if b {
			res |= 1 << i
		}
	}
	s.putByte(res)
}

func (s *serializer) putString(in string) {
	s.putByteSlice([]byte(in))
}

func (s *serializer) putByteSlice(in []byte) {
	s.putVInt(len(in)) // size of slice
	s.buf.Write(in)
}

func (s *serializer) putByte(b byte) {
	s.buf.Write([]byte{b})
}

func (s *serializer) putInt(v int) {
	s.buf.Write([]byte{
		byte(v),
		byte(v >> 8),
		byte(v >> 16),
		byte(v >> 24),
	})
}

func (s *serializer) putInt16(v int16) {
	s.buf.Write([]byte{
		byte(v),
		byte(v >> 8),
	})
}

// serializes an int with as few bytes as possible
func (s *serializer) putVInt(v int) {
	switch {
	case v < 0xfb:
		s.putByte(byte(v))
	case v >= 0xfc && v < 0xffff:
		s.putByte(0xfc)
		s.putInt16(int16(v))
	default:
		s.putByte(0xfe)
		s.putInt(v)
	}
}

func (s *serializer) putRefBool(b *bool) {
	if b == nil {
		s.putByte(0xff)
		return
	}
	s.putBool(*b)
}

func (s *serializer) putBool(b bool) {
	if b {
		s.putByte(1)
	} else {
		s.putByte(0)
	}
}
