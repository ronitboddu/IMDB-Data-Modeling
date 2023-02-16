package models

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"

	"github.com/lib/pq"
	"github.com/ronitboddu/assignment1/pkg/service"
)

//Object to map rating details from the title.ratings datafile
type Rating struct {
	tconst        string
	averageRating float32
	numVotes      int
}

const RATING_FILENAME = "./clean_title_ratings.tsv"

var Ratings []Rating

//Get all the rating objects
func GetAllRatings() []Rating {
	if len(Ratings) == 0 {
		records := service.GetRecords(RATING_FILENAME)
		ExtractAllRatings(records, &Ratings)
		return Ratings
	}
	return Ratings
}

func ExtractAllRatings(records [][]string, Ratings *[]Rating) {
	for i := 1; i < len(records); i++ {
		rating := ExtractRating(records[i])
		*Ratings = append(*Ratings, rating)
	}
}

//Map individual row into the object
func ExtractRating(record []string) Rating {
	var rating Rating

	rating.tconst = record[0]
	rating.averageRating = handleAvgRating(record[1])
	votes, err := strconv.Atoi(record[2])
	if err != nil {
		fmt.Println("error in converting numVotes to int")
		os.Exit(0)
	}
	rating.numVotes = votes
	return rating
}

func handleAvgRating(avgRating string) float32 {
	value, err := strconv.ParseFloat(avgRating, 32)
	if err != nil {
		fmt.Println("error in converting avg rating to float32")
		os.Exit(0)
	}
	return float32(value)

}

// Insert the data into the ratings table
func InsertRatings(db *sql.DB) {
	fmt.Println("Ratings Insertion started!!!")
	ratings := GetAllRatings()
	if len(Map_tid.M) == 0 {
		MapTid()
	}
	txn, err := db.Begin()
	CheckError(err)
	stmt, _ := txn.Prepare(pq.CopyIn("ratings", "tid", "avg_rating", "num_votes"))
	for i := 0; i < len(ratings); i++ {
		tid := GetUniqueTID(ratings[i].tconst)
		if Map_tid.Contains(tid) {
			//fmt.Println(ratings[i].averageRating)
			_, err := stmt.Exec(tid, ratings[i].averageRating, ratings[i].numVotes)
			if err != nil {
				fmt.Println(ratings[i].averageRating)
				fmt.Println(err)
				os.Exit(0)
			}
		}
	}
	_, err = stmt.Exec()
	CheckError(err)
	err = stmt.Close()
	CheckError(err)
	err = txn.Commit()
	CheckError(err)
	fmt.Println("Inserted Ratings successfully!!")
}
