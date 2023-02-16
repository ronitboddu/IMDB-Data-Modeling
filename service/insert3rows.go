package service

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

//will insert 3 rows, with non compatible data in 2nd row which should throw error and should bring back Databse
// to it's original state
func Insert3Rows(db *sql.DB) {
	fmt.Println("Inserting 3 rows in movies table.")
	txn, err := db.Begin()
	CheckError(err)
	stmt, _ := txn.Prepare(pq.CopyIn("movies", "tid", "title", "title_type", "is_adult", "release_year", "runtime"))
	_, err = stmt.Exec(26654390, "The Man from Earth", "movie", "N", 2007, 132)
	if err != nil {
		fmt.Println("error inserting row 1")
		return
	}
	//Violating unique key constraint
	_, err = stmt.Exec(1, "The Judge", "movie", "N", 2014, 120)
	if err != nil {
		fmt.Println("error inserting row 2")
		return
	}

	_, err = stmt.Exec(26654391, "12 Angry Men", "movie", "N", 1957, 150)
	if err != nil {
		fmt.Println("error inserting row 3")
		return
	}
	_, err = stmt.Exec()
	if err != nil {
		fmt.Println("error inserting row 2")
		return
	}
	err = stmt.Close()
	CheckError(err)
	err = txn.Commit()
	CheckError(err)
	fmt.Println("Inserted Movies successfully!!")
}

func CheckDataState(db *sql.DB) {
	// check whether row 1 is inserted
	rows, err := db.Query("SELECT * FROM movies where tid=26654390")
	CheckError(err)
	defer rows.Close()
	if !rows.Next() {
		fmt.Println("row 1 not found in the Database")
	}
	fmt.Println("Database state is retained!!")
}
