package sqlparser

import (
	"fmt"
)

func Deserialize(in []byte) Statement {
	d := &deserializer{buf: in}

	return d.deserializeStatement()
}

type deserializer struct {
	buf []byte
	pos int
}

func (d *deserializer) deserializeStatement() Statement {
	b := d.readByte()
	switch b {
	case tSelect:
		cache := d.readBoolRef()
		bools := d.readBools(11)
		distinct := bools[0]
		straightJoinHint := bools[1]
		sqlCalcFoundRows := bools[2]
		var comments Comments
		if !bools[3] {
			comments = d.deserializeComment()
		}
		var expressions SelectExprs
		if !bools[4] {
			exprSize := d.readVint()
			expressions = make(SelectExprs, exprSize)
			for i := 0; i < exprSize; i++ {
				expressions[i] = d.deserializeSelectExpr()
			}
		}
		var tableExprs TableExprs
		if !bools[5] {
			fromSize := d.readVint()
			tableExprs = make(TableExprs, fromSize)
			for i := 0; i < fromSize; i++ {
				tableExprs[i] = d.deserializeTableExpr()
			}
		}

		return &Select{
			Comments:         comments,
			Cache:            cache,
			Distinct:         distinct,
			StraightJoinHint: straightJoinHint,
			SQLCalcFoundRows: sqlCalcFoundRows,
			SelectExprs:      expressions,
			From:             tableExprs,
		}
	default:
		panic(fmt.Sprintf("unknown type %d", b))
	}
}

func (d *deserializer) readVint() int {
	b := d.readByte()
	switch {
	case b < 0xfb:
		return int(b)
	case b == 0xfc:
		return int(d.readInt16())
	case b == 0xfe:
		return d.readInt()
	}
	panic(b)
}

func (d *deserializer) readByte() byte {
	return d.buf[d.nextPos()]
}

func (d *deserializer) readBools(size int) []bool {
	var recursiveResult []bool
	for size > 8 {
		// when we are dealing with arrays larger than a single byte, we 
		// go into recursive mode and read one byte at the time
		recursiveResult = append(recursiveResult, d.readBools(8)...)
		size -= 8
	}

	byt := d.readByte()
	chunk := make([]bool, size)
	for i := 0; i < size; i++ {
		b := (byt & (1 << i)) == 0
		chunk[i] = !b
	}

	if recursiveResult == nil {
		return chunk
	}
	return append(recursiveResult, chunk...)
}

func (d *deserializer) readBoolRef() *bool {
	b := d.readByte()
	if b == 0xff {
		return nil
	}

	res := b == 0x01
	return &res
}

func (d *deserializer) nextPos() int {
	p := d.pos
	d.pos = d.pos + 1
	return p
}

func (d *deserializer) readByteSlice() []byte {
	size := d.readVint()
	startPos := d.pos
	d.pos = d.pos + size
	return d.buf[startPos:d.pos]
}

func (d *deserializer) deserializeColdIdent() ColIdent {
	val := string(d.readByteSlice())
	return ColIdent{
		val: val,
	}
}

func (d *deserializer) deserializeComment() Comments {
	size := d.readVint()
	result := make(Comments, size)
	for i := 0; i < size; i++ {
		row := d.readByteSlice()
		result[i] = row
	}
	return result
}
func (d *deserializer) deserializeSelectExpr() SelectExpr {
	b := d.readByte()
	if b == 0 {
		return nil
	}
	switch b {
	case tAliasedExpr:
		return &AliasedExpr{
			As:   d.deserializeColdIdent(),
			Expr: d.deserializeExpr(),
		}
	default:
		panic("unknown SelectExpr")
	}
}

func (d *deserializer) deserializeExpr() Expr {
	b := d.readByte()
	switch b {
	case tLiteral:
		typ := ValType(d.readByte())
		val := d.readByteSlice()
		return &Literal{
			Type: typ,
			Val:  val,
		}
	}
	panic("oh noes")
}

func (d *deserializer) readInt() int {
	b := d.buf[d.pos : d.pos+4]
	d.pos = d.pos + 4
	return int(b[0]) |
		int(b[1])<<8 |
		int(b[2])<<16 |
		int(b[3])<<24
}

func (d *deserializer) readInt16() int16 {
	b1 := d.readByte()
	b2 := d.readByte()
	return int16(b1) | int16(b2)<<8
}

func (d *deserializer) deserializeTableExpr() TableExpr {
	switch d.readByte() {
	case tAliasedTableExpr:
		expr := d.deserializeSimpleTableExpr()
		return &AliasedTableExpr{Expr: expr}
	}
	panic("not implemented")
}

func (d *deserializer) deserializeSimpleTableExpr() SimpleTableExpr {
	switch d.readByte() {
	case tTableName:
		val := string(d.readByteSlice())
		return TableName{Name: TableIdent{v: val}}
	}

	panic("not implemented")
}
