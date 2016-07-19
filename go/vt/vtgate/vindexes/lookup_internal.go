package vindexes

import (
	"fmt"
)

// lookup implements the functions for the Lookup vindexes.
type lookup struct {
	Table              string `json:"table"`
	From               string `json:"from"`
	To                 string `json:"to"`
	sel, ver, ins, del string
	isHashedIndex      bool
}

func (lkp *lookup) Init(m map[string]string, isHashed bool) {
	t := m["table"]
	from := m["from"]
	to := m["to"]

	lkp.Table = t
	lkp.From = from
	lkp.To = to
	lkp.sel = fmt.Sprintf("select %s from %s where %s = :%s", to, t, from, from)
	lkp.ver = fmt.Sprintf("select %s from %s where %s = :%s and %s = :%s", from, t, from, from, to, to)
	lkp.ins = fmt.Sprintf("insert into %s(%s, %s) values(:%s, :%s)", t, from, to, from, to)
	lkp.del = fmt.Sprintf("delete from %s where %s = :%s and %s = :%s", t, from, from, to, to)
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

// Verify returns true if id maps to ksid.
func (lkp *lookup) Verify(vcursor VCursor, id interface{}, ksid []byte) (bool, error) {
	var val interface{}
	var err error
	if lkp.isHashedIndex {
		val, err = vunhash(ksid)
		if err != nil {
			return false, fmt.Errorf("lookup.Verify: %v", err)
		}
	} else {
		val = ksid
	}
	result, err := vcursor.Execute(lkp.ver, map[string]interface{}{
		lkp.From: id,
		lkp.To:   val,
	})
	if err != nil {
		return false, fmt.Errorf("lookup.Verify: %v", err)
	}
	if len(result.Rows) == 0 {
		return false, nil
	}
	return true, nil
}

// Create creates an association between id and ksid by inserting a row in the vindex table.
func (lkp *lookup) Create(vcursor VCursor, id interface{}, ksid []byte) error {
	var val interface{}
	var err error
	if lkp.isHashedIndex {
		val, err = vunhash(ksid)
		if err != nil {
			return fmt.Errorf("lookup.Create: %v", err)
		}
	} else {
		val = ksid
	}
	if _, err := vcursor.Execute(lkp.ins, map[string]interface{}{
		lkp.From: id,
		lkp.To:   val,
	}); err != nil {
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
