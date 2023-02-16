package models

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

//Insert the data in movie_genre table
func InsertGenres(db *sql.DB) {
	fmt.Println("Genre Insertion Started!!!")
	movies := getAllMovies()
	if len(Map_tid.M) == 0 {
		MapTid()
	}
	txn, err := db.Begin()
	CheckError(err)
	stmt, _ := txn.Prepare(pq.CopyIn("movie_genre", "tid", "genre"))
	for i := 0; i < len(movies); i++ {
		tid := GetUniqueTID(movies[i].tconst)
		if Map_tid.Contains(tid) {
			for j := 0; j < len(movies[i].genres); j++ {
				_, err := stmt.Exec(tid, movies[i].genres[j])
				CheckError(err)
			}
		}
	}
	_, err = stmt.Exec()
	CheckError(err)
	err = stmt.Close()
	CheckError(err)
	err = txn.Commit()
	CheckError(err)
	fmt.Println("Inserted Genres successfully!!")
}
