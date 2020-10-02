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
		bools := d.readBools()
		distinct := bools[0]
		straightJoinHint := bools[1]
		sqlCalcFoundRows := bools[2]
		nils := d.readBools()
		var expressions SelectExprs
		if !nils[1] {
			exprSize := d.readVint()
			expressions = make(SelectExprs, exprSize)
			for i := 0; i < exprSize; i++ {
				expressions[i] = d.deserializeSelectExpr()
			}
		}
		var tableExprs TableExprs
		if !nils[1] {
			fromSize := d.readVint()
			tableExprs = make(TableExprs, fromSize)
			for i := 0; i < fromSize; i++ {
				tableExprs[i] = d.deserializeTableExpr()
			}
		}

		return &Select{
			Cache:            cache,
			Distinct:         distinct,
			StraightJoinHint: straightJoinHint,
			SQLCalcFoundRows: sqlCalcFoundRows,
			Comments:         nil,
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

func (d *deserializer) readBools() []bool {
	res := make([]bool, 8)
	byt := d.readByte()
	for i := 0; i < 8; i++ {
		b := (byt & (1 << i)) == 0
		res[i] = !b
	}
	return res
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
