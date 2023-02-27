/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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

// ContainsLockStatement returns true if the query contains a Get Lock statement.
func ContainsLockStatement(stmt Statement) bool {
	switch stmt := stmt.(type) {
	case *Select:
		return isLockStatement(stmt)
	case *Union:
		return isLockStatement(stmt.Left) || isLockStatement(stmt.Right)
	}

	return false
}

// isLockStatement returns true if the query is a Get Lock statement.
func isLockStatement(stmt Statement) bool {
	if s, ok := stmt.(*Select); !ok {
		return false
	} else {
		foundLastInsertId := false
		err := Walk(func(node SQLNode) (kontinue bool, err error) {
			switch node.(type) {
			case *LockingFunc:
				foundLastInsertId = true
				return false, nil
			}
			return true, nil
		}, s)
		if err != nil {
			return false
		}
		return foundLastInsertId
	}
}

func hasFuncInStatement(funcs []string, stmt Statement) bool {
	//return false if stmt is not a Select statement
	if s, ok := stmt.(*Select); !ok {
		return false
	} else {
		//visit the select statement and check if it is a Select Last Insert ID statement
		foundLastInsertId := false
		err := Walk(func(node SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *FuncExpr:
				for _, f := range funcs {
					if node.Name.Lowered() == f {
						foundLastInsertId = true
						return false, nil
					}
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
	return hasFuncInStatement([]string{"last_insert_id"}, stmt)
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
