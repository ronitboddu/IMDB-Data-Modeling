package models

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
	"github.com/ronitboddu/assignment1/pkg/service"
)

// Object to Map entries from title.basics data file
type Movie struct {
	tconst         string
	titleType      string
	primaryTitle   string
	originalTitle  string
	isAdult        string
	startYear      int
	endYear        string
	runtimeMinutes int
	genres         []string
}

const MOVIES_FILENAME = "./clean_title_basics.tsv"

var Movies []Movie

// Get array of objects
func getAllMovies() []Movie {
	if len(Movies) == 0 {
		records := service.GetRecords(MOVIES_FILENAME)
		ExtractAllMovies(records, &Movies)
		return Movies
	}
	return Movies
}

//Extract all movies
func ExtractAllMovies(records [][]string, Movies *[]Movie) {
	for i := 1; i < len(records); i++ {
		if records[i][1] == "tvEpisode" || records[i][4] == "1" {
			continue
		}
		movie := ExtractMovie(records[i])
		*Movies = append(*Movies, movie)
	}
}

//Map individual row from the title.basics file to the object
func ExtractMovie(record []string) Movie {
	var movie Movie
	movie.tconst = record[0]
	movie.titleType = record[1]
	movie.primaryTitle = record[2]
	movie.originalTitle = record[3]
	movie.isAdult = handleIsAdult(record[4])
	movie.startYear = handleYear(record[5])
	movie.endYear = record[6]
	movie.runtimeMinutes = handleRuntimeValue(record[7])
	movie.genres = strings.Split(strings.TrimSpace(record[8]), ",")
	return movie
}

func handleRuntimeValue(value string) int {
	runtime, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return runtime
}

func handleIsAdult(value string) string {
	if value == "1" {
		return "Y"
	}
	return "N"
}

func handleYear(value string) int {
	startYear, err := strconv.Atoi(value)
	if err != nil {
		return 9999
	}
	return startYear
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}

//Insert the data into the movies table
func InsertMoviesInDB(db *sql.DB) {
	fmt.Println("Movie Insertion started!!!")
	movies := getAllMovies()
	txn, err := db.Begin()
	CheckError(err)
	stmt, _ := txn.Prepare(pq.CopyIn("movies", "tid", "title", "title_type", "is_adult", "release_year", "runtime"))
	for i := 0; i < len(movies); i++ {
		_, err := stmt.Exec(GetUniqueTID(movies[i].tconst), movies[i].originalTitle, movies[i].titleType,
			movies[i].isAdult, movies[i].startYear, movies[i].runtimeMinutes)
		CheckError(err)
	}
	_, err = stmt.Exec()
	CheckError(err)
	err = stmt.Close()
	CheckError(err)
	err = txn.Commit()
	CheckError(err)
	fmt.Println("Inserted Movies successfully!!")
}

//Extract unique Integer value from tconst
func GetUniqueTID(tconst string) int {
	id, err := strconv.Atoi(tconst[2:])
	CheckError(err)
	return id
}
