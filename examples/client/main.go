package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func insertExample(conn *sql.DB) error {
	stmt, err := conn.Prepare("INSERT INTO users (name) VALUES (?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec("test@example.com")
	if err != nil {
		return err
	}

	return nil
}

func main() {
	db, err := sql.Open("mysql", "user:password@tcp(127.0.0.1:3306)/database")
	if err != nil {
		fmt.Println("Error connecting to database:", err)
		return
	}
	defer db.Close()

	err = insertExample(db)
	if err != nil {
		fmt.Println("Error inserting example:", err)
		return
	}

	fmt.Println("Example inserted successfully")
} 