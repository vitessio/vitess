package vtgate

import (
	"bytes"
	"strings"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// InsertFunc insert callback
type InsertFunc func(insert string) error

// NewLoadData new LoadData obj
func NewLoadData() *LoadData {
	return &LoadData{
		LoadDataInfo: &LoadDataInfo{
			maxRowsInBatch: *loadMaxRowsInBatch,
		},
	}
}

// LoadData a meta struct that contains the load data.
type LoadData struct {
	LoadDataInfo *LoadDataInfo
}

// LoadDataInfo params
type LoadDataInfo struct {
	maxRowsInBatch int
	LinesInfo      *sqlparser.LinesClause
	FieldsInfo     *sqlparser.FieldsClause
	Columns        sqlparser.Columns
	Table          *sqlparser.TableName
}

// SetMaxRowsInBatch sets the max affected records of batch insert.
func (l *LoadDataInfo) SetMaxRowsInBatch(limit int) {
	l.maxRowsInBatch = limit
}

// ParseLoadDataPram parse the load data statement
func (l *LoadDataInfo) ParseLoadDataPram(loadStmt *sqlparser.LoadDataStmt) {
	l.Columns = loadStmt.Columns
	l.Table = &loadStmt.Table
	l.FieldsInfo = loadStmt.FieldsInfo
	l.LinesInfo = loadStmt.LinesInfo

}

// getValidData returns prevData and curData that starts from starting symbol.
// If the data doesn't have starting symbol, prevData is nil and curData is curData[len(curData)-startingLen+1:].
// If curData size less than startingLen, curData is returned directly.
func (l *LoadDataInfo) getValidData(prevData, curData []byte) ([]byte, []byte) {
	startingLen := len(l.LinesInfo.Starting)
	if startingLen == 0 {
		return prevData, curData
	}

	prevLen := len(prevData)
	if prevLen > 0 {
		// starting symbol in the prevData
		idx := strings.Index(string(prevData), l.LinesInfo.Starting)
		if idx != -1 {
			return prevData[idx:], curData
		}

		// starting symbol in the middle of prevData and curData
		restStart := curData
		if len(curData) >= startingLen {
			restStart = curData[:startingLen-1]
		}
		prevData = append(prevData, restStart...)
		idx = strings.Index(string(prevData), l.LinesInfo.Starting)
		if idx != -1 {
			return prevData[idx:prevLen], curData
		}
	}

	// starting symbol in the curData
	idx := strings.Index(string(curData), l.LinesInfo.Starting)
	if idx != -1 {
		return nil, curData[idx:]
	}

	// no starting symbol
	if len(curData) >= startingLen {
		curData = curData[len(curData)-startingLen+1:]
	}
	return nil, curData
}

// getLine returns a line, curData, the next data start index and a bool value.
// If it has starting symbol the bool is true, otherwise is false.
func (l *LoadDataInfo) getLine(prevData, curData []byte) ([]byte, []byte, bool) {
	startingLen := len(l.LinesInfo.Starting)
	prevData, curData = l.getValidData(prevData, curData)
	if prevData == nil && len(curData) < startingLen {
		return nil, curData, false
	}
	prevLen := len(prevData)
	terminatedLen := len(l.LinesInfo.Terminated)
	curStartIdx := 0
	if prevLen < startingLen {
		curStartIdx = startingLen - prevLen
	}
	endIdx := -1
	if len(curData) >= curStartIdx {
		endIdx = strings.Index(string(curData[curStartIdx:]), l.LinesInfo.Terminated)
	}
	if endIdx == -1 {
		// no terminated symbol
		if len(prevData) == 0 {
			return nil, curData, true
		}
		// terminated symbol in the middle of prevData and curData
		curData = append(prevData, curData...)
		endIdx = strings.Index(string(curData[startingLen:]), l.LinesInfo.Terminated)
		if endIdx != -1 {
			nextDataIdx := startingLen + endIdx + terminatedLen
			return curData[startingLen : startingLen+endIdx], curData[nextDataIdx:], true
		}
		// no terminated symbol
		return nil, curData, true
	}
	// terminated symbol in the curData
	nextDataIdx := curStartIdx + endIdx + terminatedLen
	if len(prevData) == 0 {
		return curData[curStartIdx : curStartIdx+endIdx], curData[nextDataIdx:], true
	}
	// terminated symbol in the curData
	prevData = append(prevData, curData[:nextDataIdx]...)
	endIdx = strings.Index(string(prevData[startingLen:]), l.LinesInfo.Terminated)
	if endIdx >= prevLen {
		return prevData[startingLen : startingLen+endIdx], curData[nextDataIdx:], true
	}

	// terminated symbol in the middle of prevData and curData
	lineLen := startingLen + endIdx + terminatedLen
	return prevData[startingLen : startingLen+endIdx], curData[lineLen-prevLen:], true
}

// MakeInsert  batch "insert ignore into" to implement the  load data
func (l *LoadDataInfo) MakeInsert(rows [][]string, tb *vindexes.Table, fields []*querypb.Field) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var insertVasSQLBuf bytes.Buffer
	insertVasSQLBuf.WriteString("INSERT  IGNORE  INTO ")
	insertVasSQLBuf.WriteString(l.Table.Name.String())

	//get fields type and make the  insert ignore into values(...)
	columns := l.Columns
	columnsSize := len(columns)
	var specificFields = make([]*querypb.Field, 0, len(columns))
	insertVasSQLBuf.WriteString("(")

	// identify columns is not necessary
	if columns != nil && columnsSize > 0 {
		for key, value := range columns {
			for _, field := range fields {
				if strings.EqualFold(field.Name, value.String()) {
					specificFields = append(specificFields, field)
				}
			}
			insertVasSQLBuf.WriteString(value.String())
			if key != columnsSize-1 {
				insertVasSQLBuf.WriteString(",")
			}
		}
		insertVasSQLBuf.WriteString(") ")
	} else {
		specificFields = fields
		columnsSize = len(specificFields)
		for key, e := range specificFields {
			insertVasSQLBuf.WriteString(e.Name)
			if key != columnsSize-1 {
				insertVasSQLBuf.WriteString(",")
			}
		}
		insertVasSQLBuf.WriteString(") ")
	}
	insertVasSQLBuf.WriteString("values")
	for r, record := range rows {
		insertVasSQLBuf.WriteString("(")

		for k, specificField := range specificFields {
			var column string
			if k >= len(record) {
				column = ""
			} else {
				column = record[k]
			}

			// Distinguish the type of  field. number or char.
			// if number eg:insert values(100);
			// if string eg:insert values('100')
			if isNumericType(specificField.Type, column) {
				insertVasSQLBuf.WriteString(column)
			} else {
				insertVasSQLBuf.WriteString("'" + column + "'")
			}
			if k != columnsSize-1 {
				insertVasSQLBuf.WriteString(",")
			}
		}

		if r == len(rows)-1 {
			insertVasSQLBuf.WriteString(")")
		} else {
			insertVasSQLBuf.WriteString("),")
		}
	}
	return insertVasSQLBuf.String(), nil
}

// GetRowFromLine splits line according to fieldsInfo, this function is exported for testing.
func (l *LoadDataInfo) GetRowFromLine(line []byte) ([]string, error) {
	var sep []byte
	if l.FieldsInfo.Enclosed != 0 {
		if line[0] != l.FieldsInfo.Enclosed || line[len(line)-1] != l.FieldsInfo.Enclosed {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "line %s should begin and end with %v", string(line), l.FieldsInfo.Enclosed)
		}
		line = line[1 : len(line)-1]
		sep = make([]byte, 0, len(l.FieldsInfo.Terminated)+2)
		sep = append(sep, l.FieldsInfo.Enclosed)
		sep = append(sep, l.FieldsInfo.Terminated...)
		sep = append(sep, l.FieldsInfo.Enclosed)
	} else {
		sep = []byte(l.FieldsInfo.Terminated)
	}
	rawCols := bytes.Split(line, sep)
	cols := escapeCols(rawCols)
	return cols, nil
}

// InsertData inserts data into specified table according to the specified format.
// If prevData isn't nil and curData is nil, there are no other data to deal with and the isEOF is true
func (l *LoadDataInfo) InsertData(prevData, curData []byte, rows *[][]string, tb *vindexes.Table, fields []*querypb.Field, callback InsertFunc) ([]byte, bool, error) {
	// TODO: support enclosed and escape.
	if len(prevData) == 0 && len(curData) == 0 {
		return nil, false, nil
	}
	var line []byte
	var isEOF, hasStarting, reachLimit bool
	if len(prevData) > 0 && len(curData) == 0 {
		isEOF = true
		prevData, curData = curData, prevData
	}
	for len(curData) > 0 {
		line, curData, hasStarting = l.getLine(prevData, curData)
		prevData = nil
		// If it doesn't find the terminated symbol and this data isn't the last data,
		// the data can't be inserted.
		if line == nil && !isEOF {
			break
		}
		// If doesn't find starting symbol, this data can't be inserted.
		if !hasStarting {
			if isEOF {
				curData = nil
			}
			break
		}
		if line == nil && isEOF {
			line = curData[len(l.LinesInfo.Starting):]
			curData = nil
		}

		cols, err := l.GetRowFromLine(line)
		if err != nil {
			return nil, false, err
		}
		*rows = append(*rows, cols)
		if l.maxRowsInBatch != 0 && len(*rows) == l.maxRowsInBatch {
			reachLimit = true
			var inserts string
			if inserts, err = l.MakeInsert(*rows, tb, fields); err != nil {
				return nil, false, err
			}
			*rows = make([][]string, 0, *loadMaxRowsInBatch)
			if err := callback(inserts); err != nil {
				return nil, false, err
			}
		}
	}
	return curData, reachLimit, nil
}

func (l *LoadDataInfo) insertDataWithBatch(prevData, curData []byte, rows *[][]string, tb *vindexes.Table, fields []*querypb.Field, callback InsertFunc) ([]byte, error) {
	var err error
	var reachLimit bool
	for {
		prevData, reachLimit, err = l.InsertData(prevData, curData, rows, tb, fields, callback)
		if err != nil {
			return nil, err
		}
		if !reachLimit {
			break
		}
		curData = prevData
		prevData = nil
	}
	return prevData, nil
}

func escapeCols(strs [][]byte) []string {
	ret := make([]string, len(strs))
	for i, v := range strs {
		tmpV := string(v)
		if tmpV == "\\N" || tmpV == "\\N\r" {
			v = []byte("NULL")
		}
		output := escape(v)
		ret[i] = string(output)
	}
	return ret
}

// escape handles escape characters when running load data statement.
// TODO: escape need to be improved, it should support ESCAPED BY to specify
// the escape character and handle \N escape.
// See http://dev.mysql.com/doc/refman/5.7/en/load-data.html
func escape(str []byte) []byte {
	desc := make([]byte, len(str)*2)
	pos := 0
	for i := 0; i < len(str); i++ {
		c := str[i]
		if c == '\\' && i+1 < len(str) {
			c = sqlparser.EscapeChar(str[i+1])
			desc[pos] = c
			i++
			pos++
		} else {
			desc[pos] = c
			pos++
		}
	}
	return desc[:pos]
}

func isNumericType(t querypb.Type, column string) bool {
	if column == "NULL" {
		return true
	}
	if t == querypb.Type_INT8 ||
		t == querypb.Type_INT16 ||
		t == querypb.Type_INT24 ||
		t == querypb.Type_INT32 ||
		t == querypb.Type_INT64 ||
		t == querypb.Type_UINT8 ||
		t == querypb.Type_UINT16 ||
		t == querypb.Type_UINT24 ||
		t == querypb.Type_UINT32 ||
		t == querypb.Type_UINT64 ||
		t == querypb.Type_FLOAT32 ||
		t == querypb.Type_FLOAT64 ||
		t == querypb.Type_DECIMAL {
		return true
	}
	return false
}
