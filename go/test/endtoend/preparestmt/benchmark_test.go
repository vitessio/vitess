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

package preparestmt

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/brianvoe/gofakeit/v7"
)

/*
export ver=v1 p=~/benchmark && go test \
-run '^$' -bench '^BenchmarkPreparedStmt' \
-benchtime 2s -count 6 -cpu 4 \
| tee $p/${ver}.txt
*/
func BenchmarkPreparedStmt(b *testing.B) {
	dbo := Connect(b)
	defer dbo.Close()

	// prepare statement
	insertStmt := `insert into sks.t1 (name, age, email, created_at, is_active) values(?,  ?,  ?,  current_timestamp,  ?)`
	selectStmt := `select id, name, age, email from sks.t1 where age between ? and ? and is_active = ? limit ?`
	updateStmt := `update sks.t1 set is_active = ? where id = ?`
	deleteStmt := `delete from sks.t1 where is_active = ? and age = ?`

	joinStmt := `SELECT 
    user.id AS user_id
FROM 
    sks.t1 AS user
LEFT JOIN 
    sks.t1 AS parent ON user.id = parent.id AND parent.age = ?
LEFT JOIN 
    sks.t1 AS manager ON user.id = manager.id AND manager.is_active = ?
LEFT JOIN 
    sks.t1 AS child ON user.id = child.id
WHERE 
    user.is_active = ? 
    AND user.id = ?
    AND parent.id = ?
    AND manager.id = ?`

	iStmt, err := dbo.Prepare(insertStmt)
	if err != nil {
		b.Fatal(err)
	}
	defer iStmt.Close()

	b.Run("Insert", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := iStmt.Exec(gofakeit.FirstName(), rand.IntN(100), gofakeit.Email(), rand.IntN(2))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	sStmt, err := dbo.Prepare(selectStmt)
	if err != nil {
		b.Fatal(err)
	}
	defer sStmt.Close()

	b.Run("Select", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			age := rand.IntN(80)
			r, err := sStmt.Query(age, age+20, rand.IntN(2), rand.IntN(10))
			if err != nil {
				b.Fatal(err)
			}
			r.Close()
		}
	})

	jStmt, err := dbo.Prepare(joinStmt)
	if err != nil {
		b.Fatal(err)
	}
	defer jStmt.Close()

	b.Run("Join Select:Simple Route", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			age := rand.IntN(80)
			active := rand.IntN(2)
			id := rand.IntN(2000)
			r, err := jStmt.Query(age, active, active, id, id, id)
			if err != nil {
				b.Fatal(err)
			}
			r.Close()
		}
	})

	uStmt, err := dbo.Prepare(updateStmt)
	if err != nil {
		b.Fatal(err)
	}
	defer uStmt.Close()

	b.Run("Update", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err = uStmt.Exec(rand.IntN(2), rand.IntN(2000))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	dStmt, err := dbo.Prepare(deleteStmt)
	if err != nil {
		b.Fatal(err)
	}
	defer dStmt.Close()

	b.Run("Delete", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err = dStmt.Exec(rand.IntN(2), rand.IntN(100))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

/*
export ver=v1 p=~/benchmark && go test \
-run '^$' -bench '^BenchmarkExecuteStmt' \
-benchtime 2s -count 6 -cpu 4 \
| tee $p/${ver}.txt
*/
func BenchmarkExecuteStmt(b *testing.B) {
	dbo := Connect(b, "interpolateParams=false")
	defer dbo.Close()

	// prepare statement
	insertStmt := `insert into sks.t1 (name, age, email, created_at, is_active) values('%s', %d, '%s', current_timestamp, %d)`
	selectStmt := `select id, name, age, email from sks.t1 where age between %d and %d and is_active = %d limit %d`
	updateStmt := `update sks.t1 set is_active = %d where id = %d`
	deleteStmt := `delete from sks.t1 where is_active = %d and age = %d`

	joinStmt := `SELECT 
    user.id AS user_id
FROM 
    sks.t1 AS user
LEFT JOIN 
    sks.t1 AS parent ON user.id = parent.id AND parent.age = %d
LEFT JOIN 
    sks.t1 AS manager ON user.id = manager.id AND manager.is_active = %d
LEFT JOIN 
    sks.t1 AS child ON user.id = child.id
WHERE 
    user.is_active = %d 
    AND user.id = %d
    AND parent.id = %d
    AND manager.id = %d`

	ctx := b.Context()

	b.Run("Insert", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := dbo.ExecContext(ctx, fmt.Sprintf(insertStmt, gofakeit.FirstName(), rand.IntN(100), gofakeit.Email(), rand.IntN(2)))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Select", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			age := rand.IntN(80)
			r, err := dbo.QueryContext(ctx, fmt.Sprintf(selectStmt, age, age+20, rand.IntN(2), rand.IntN(10)))
			if err != nil {
				b.Fatal(err)
			}
			r.Close()
		}
	})

	b.Run("Join Select:Simple Route", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			age := rand.IntN(80)
			active := rand.IntN(2)
			id := rand.IntN(2000)
			r, err := dbo.QueryContext(ctx, fmt.Sprintf(joinStmt, age, active, active, id, id, id))
			if err != nil {
				b.Fatal(err)
			}
			r.Close()
		}
	})

	b.Run("Update", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := dbo.ExecContext(ctx, fmt.Sprintf(updateStmt, rand.IntN(2), rand.IntN(2000)))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Delete", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := dbo.ExecContext(ctx, fmt.Sprintf(deleteStmt, rand.IntN(2), rand.IntN(100)))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
