/*
Copyright 2024 The Vitess Authors.

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

package schemadiff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestAnnotateAll(t *testing.T) {
	stmt := `create table t(
	id int,
	name varchar(100),
	primary key(id)
) engine=innodb`
	annotations := annotateAll(stmt, RemovedTextualAnnotationType)
	assert.Equal(t, 5, annotations.Len())
	expect := `-create table t(
-	id int,
-	name varchar(100),
-	primary key(id)
-) engine=innodb`
	assert.Equal(t, expect, annotations.Export())
}

func TestUnifiedAnnotated(t *testing.T) {
	tcases := []struct {
		name            string
		from            string
		to              string
		fromAnnotations *TextualAnnotations
		toAnnotations   *TextualAnnotations
		expected        string
	}{
		{
			"no change",
			"CREATE TABLE t1 (a int)",
			"CREATE TABLE t1 (a int)",
			&TextualAnnotations{},
			&TextualAnnotations{},
			"CREATE TABLE `t1` (\n\t`a` int\n)",
		},
		{
			"simple",
			"CREATE TABLE t1 (a int)",
			"CREATE TABLE t1 (a int, b int)",
			&TextualAnnotations{},
			&TextualAnnotations{texts: []*AnnotatedText{{text: "`b` int", typ: AddedTextualAnnotationType}}},
			" CREATE TABLE `t1` (\n \t`a` int\n+\t`b` int\n )",
		},
	}
	parser := sqlparser.NewTestParser()

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			fromStmt, err := parser.ParseStrictDDL(tcase.from)
			require.NoError(t, err)
			annotatedFrom := annotatedStatement(sqlparser.CanonicalString(fromStmt), RemovedTextualAnnotationType, tcase.fromAnnotations)
			toStmt, err := parser.ParseStrictDDL(tcase.to)
			require.NoError(t, err)
			annotatedTo := annotatedStatement(sqlparser.CanonicalString(toStmt), AddedTextualAnnotationType, tcase.toAnnotations)
			unified := unifiedAnnotated(annotatedFrom, annotatedTo)
			export := unified.Export()
			assert.Equalf(t, tcase.expected, export, "from: %v, to: %v", annotatedFrom.Export(), annotatedTo.Export())
		})
	}
}

func TestUnifiedAnnotatedAll(t *testing.T) {
	stmt := `create table t(
	id int,
	name varchar(100),
	primary key(id)
) engine=innodb`
	annotatedTo := annotateAll(stmt, AddedTextualAnnotationType)
	annotatedFrom := NewTextualAnnotations()
	unified := unifiedAnnotated(annotatedFrom, annotatedTo)
	expect := `+create table t(
+	id int,
+	name varchar(100),
+	primary key(id)
+) engine=innodb`
	assert.Equal(t, expect, unified.Export())
}
