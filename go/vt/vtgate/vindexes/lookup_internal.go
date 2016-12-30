package vindexes

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// lookup implements the functions for the Lookup vindexes.
type lookup struct {
	Table              string `json:"table"`
	From               string `json:"from"`
	To                 string `json:"to"`
	sel, ver, ins, del string
	isHashedIndex      bool
}

func (lkp *lookup) Init(lookupQueryParams map[string]string, isHashed bool) {
	table := lookupQueryParams["table"]
	fromCol := lookupQueryParams["from"]
	toCol := lookupQueryParams["to"]

	lkp.Table = table
	lkp.From = fromCol
	lkp.To = toCol
	lkp.sel = fmt.Sprintf("select %s from %s where %s = :%s", toCol, table, fromCol, fromCol)
	lkp.ver = fmt.Sprintf("select %s from %s where %s = :%s and %s = :%s", fromCol, table, fromCol, fromCol, toCol, toCol)
	lkp.ins = fmt.Sprintf("insert into %s(%s, %s) values", table, fromCol, toCol)
	lkp.del = fmt.Sprintf("delete from %s where %s = :%s and %s = :%s", table, fromCol, fromCol, toCol, toCol)
	lkp.isHashedIndex = isHashed
}

// MapUniqueLookup is for a Unique Vindex.
func (lkp *lookup) MapUniqueLookup(vcursor VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		result, err := vcursor.Execute(lkp.sel, map[string]interface{}{
			lkp.From: id,
		})
		if err != nil {
			return nil, fmt.Errorf("lookup.Map: %v", err)
		}
		if len(result.Rows) == 0 {
			out = append(out, []byte{})
			continue
		}
		if len(result.Rows) != 1 {
			return nil, fmt.Errorf("lookup.Map: unexpected multiple results from vindex %s: %v", lkp.Table, id)
		}
		if lkp.isHashedIndex {
			num, err := getNumber(result.Rows[0][0].ToNative())
			if err != nil {
				return nil, fmt.Errorf("lookup.Map: %v", err)
			}
			out = append(out, vhash(num))
		} else {
			out = append(out, result.Rows[0][0].Raw())
		}
	}
	return out, nil
}

// MapNonUniqueLookup is for a Non-Unique Vindex.
func (lkp *lookup) MapNonUniqueLookup(vcursor VCursor, ids []interface{}) ([][][]byte, error) {
	out := make([][][]byte, 0, len(ids))
	for _, id := range ids {
		result, err := vcursor.Execute(lkp.sel, map[string]interface{}{
			lkp.From: id,
		})
		if err != nil {
			return nil, fmt.Errorf("lookup.Map: %v", err)
		}
		var ksids [][]byte
		if lkp.isHashedIndex {
			for _, row := range result.Rows {
				num, err := getNumber(row[0].ToNative())
				if err != nil {
					return nil, fmt.Errorf("lookup.Map: %v", err)
				}
				ksids = append(ksids, vhash(num))
			}
		} else {
			for _, row := range result.Rows {
				ksids = append(ksids, row[0].Raw())
			}
		}
		out = append(out, ksids)
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (lkp *lookup) Verify(vcursor VCursor, ids []interface{}, ksids [][]byte) (bool, error) {
	var colBuff bytes.Buffer
	var err error
	if len(ids) != len(ksids) {
		return false, fmt.Errorf("lookup.Verify:length of ids %v doesn't match length of ksids %v", len(ids), len(ksids))
	}
	val := make([]interface{}, len(ksids))
	bindVars := make(map[string]interface{}, 2*len(ids))
	colBuff.WriteString("(")
	for rowNum, keyspaceID := range ksids {
		fromStr := lkp.From + strconv.Itoa(rowNum)
		toStr := lkp.To + strconv.Itoa(rowNum)
		colBuff.WriteString("(")
		colBuff.WriteString(lkp.From)
		colBuff.WriteString("=:")
		colBuff.WriteString(fromStr)
		colBuff.WriteString(" and ")
		colBuff.WriteString(lkp.To)
		colBuff.WriteString("=:")
		colBuff.WriteString(toStr)
		colBuff.WriteString(")or")
		if lkp.isHashedIndex {
			val[rowNum], err = vunhash(keyspaceID)
			if err != nil {
				return false, fmt.Errorf("lookup.Verify: %v", err)
			}
		} else {
			val[rowNum] = keyspaceID
		}
		bindVars[fromStr] = ids[rowNum]
		bindVars[toStr] = val[rowNum]
	}
	lkp.ver = fmt.Sprintf("select %s from %s where %s", lkp.From, lkp.Table, strings.Trim(colBuff.String(), "or")+")")
	result, err := vcursor.Execute(lkp.ver, bindVars)
	if err != nil {
		return false, fmt.Errorf("lookup.Verify: %v", err)
	}
	if len(result.Rows) != len(ids) {
		return false, nil
	}
	return true, nil
}

// Create creates an association between ids and ksids by inserting a row in the vindex table.
func (lkp *lookup) Create(vcursor VCursor, ids []interface{}, ksids [][]byte) error {
	var insBuffer bytes.Buffer
	var err error
	if len(ids) != len(ksids) {
		return fmt.Errorf("lookup.Create:length of ids %v doesn't match length of ksids %v", len(ids), len(ksids))
	}
	val := make([]interface{}, len(ksids))
	insBuffer.WriteString("insert into ")
	insBuffer.WriteString(lkp.Table)
	insBuffer.WriteString("(")
	insBuffer.WriteString(lkp.From)
	insBuffer.WriteString(",")
	insBuffer.WriteString(lkp.To)
	insBuffer.WriteString(") values")
	bindVars := make(map[string]interface{}, 2*len(ids))
	for rowNum, keyspaceID := range ksids {
		fromStr := lkp.From + strconv.Itoa(rowNum)
		toStr := lkp.To + strconv.Itoa(rowNum)
		insBuffer.WriteString("(:")
		insBuffer.WriteString(fromStr + ",:" + toStr)
		insBuffer.WriteString("),")
		if lkp.isHashedIndex {
			val[rowNum], err = vunhash(keyspaceID)
			if err != nil {
				return fmt.Errorf("lookup.Create: %v", err)
			}
		} else {
			val[rowNum] = keyspaceID
		}
		bindVars[fromStr] = ids[rowNum]
		bindVars[toStr] = val[rowNum]
	}
	lkp.ins = strings.Trim(insBuffer.String(), ",")
	if _, err := vcursor.Execute(lkp.ins, bindVars); err != nil {
		return fmt.Errorf("lookup.Create: %v", err)
	}
	return nil
}

// Delete deletes the association between ids and ksid.
func (lkp *lookup) Delete(vcursor VCursor, ids []interface{}, ksid []byte) error {
	var val interface{}
	var err error
	if lkp.isHashedIndex {
		val, err = vunhash(ksid)
		if err != nil {
			return fmt.Errorf("lookup.Delete: %v", err)
		}
	} else {
		val = ksid
	}
	bindvars := map[string]interface{}{
		lkp.To: val,
	}
	for _, id := range ids {
		bindvars[lkp.From] = id
		if _, err := vcursor.Execute(lkp.del, bindvars); err != nil {
			return fmt.Errorf("lookup.Delete: %v", err)
		}
	}
	return nil
}
