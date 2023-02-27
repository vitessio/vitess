package sqlparser

// IsPureSelectStatement returns true if the query is a Select or Union statement without any Lock.
func IsPureSelectStatement(stmt Statement) bool {
	switch stmt := stmt.(type) {
	case *Select:
		if stmt.Lock == NoLock {
			return true
		}
	case *Union:
		if stmt.Lock == NoLock {
			return true
		}
	}

	return false
}

// ContainsLastInsertIDStatement returns true if the query is a Select Last Insert ID statement.
func ContainsLastInsertIDStatement(stmt Statement) bool {
	switch stmt := stmt.(type) {
	case *Select:
		return isSelectLastInsertIDStatement(stmt)
	case *Union:
		return isSelectLastInsertIDStatement(stmt.Left) || isSelectLastInsertIDStatement(stmt.Right)
	}

	return false
}

// IsSelectLastInsertIDStatement returns true if the query is a Select Last Insert ID statement.
func isSelectLastInsertIDStatement(stmt Statement) bool {
	//return false if stmt is not a Select statement
	if s, ok := stmt.(*Select); !ok {
		return false
	} else {
		//visit the select statement and check if it is a Select Last Insert ID statement
		foundLastInsertId := false
		err := Walk(func(node SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *FuncExpr:
				if node.Name.Lowered() == "last_insert_id" {
					foundLastInsertId = true
					return false, nil
				}
			}
			return true, nil
		}, s)
		if err != nil {
			return false
		}
		return foundLastInsertId
	}
}

// IsDDLStatement returns true if the query is an DDL statement.
func IsDDLStatement(stmt Statement) bool {
	return ASTToStatementType(stmt) == StmtDDL
}

// IsSelectForUpdateStatement returns true if the query is a Select For Update statement.
func IsSelectForUpdateStatement(stmt Statement) bool {
	switch stmt := stmt.(type) {
	case *Select:
		if stmt.Lock != NoLock {
			return true
		}
	case *Union:
		if stmt.Lock != NoLock {
			return true
		}
	}

	return false
}

// IsKillStatement returns true if the query is a Kill statement.
func IsKillStatement(stmt Statement) bool {
	panic("implement me")
}
