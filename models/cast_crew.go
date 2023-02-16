package models

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/ronitboddu/assignment1/pkg/service"
)

// Object to map cast and crew details
type CastCrew struct {
	tconst   int
	nconst   int
	category string
}

//Set to check foreign key validity for the individual person and movies
var Map_pid = service.NewSet()
var Map_tid = service.NewSet()

var CastCrews []CastCrew

const PRINCIPAL_FILENAME = "./clean_title_principals.tsv"

//Maintain the SET for persons inserted in the DB
func MapPid() {
	persons := GetAllPersons()
	for i := 0; i < len(persons); i++ {
		if len(persons[i].primaryName) < 255 {
			pid := GetUniquePID(persons[i].nconst)
			Map_pid.Add(pid)
		}
	}
}

//Maintain the SET for movies inserted in the DB
func MapTid() {
	movies := getAllMovies()
	for i := 0; i < len(movies); i++ {
		tid := GetUniqueTID(movies[i].tconst)
		Map_tid.Add(tid)
	}
}

// Map all the cast and crew from the titles.principals datafile into the array of object
func GetAllCastCrew() []CastCrew {
	if len(CastCrews) == 0 {
		records := service.GetRecords(PRINCIPAL_FILENAME)
		ExtractAllCastCrew(records, &CastCrews)
		return CastCrews
	}
	return CastCrews
}

// Extract all the data from title.principals
func ExtractAllCastCrew(records [][]string, CastCrews *[]CastCrew) {
	for i := 1; i < len(records); i++ {
		if checkCategory(records[i][3]) {
			castCrew := ExtractCastCrew(records[i])
			*CastCrews = append(*CastCrews, castCrew)
		}
	}
}

// Map individual row to the object
func ExtractCastCrew(record []string) CastCrew {
	var castCrew CastCrew
	castCrew.tconst = GetUniqueTID(record[0])
	castCrew.nconst = GetUniquePID(record[2])
	castCrew.category = record[3]
	return castCrew
}

func checkCategory(category string) bool {
	if category == "director" || category == "producer" || category == "actor" || category == "writer" {
		return true
	}
	return false
}

//Insert the Data in cast_crew table
func InsertCastCrewInDB(db *sql.DB) {
	fmt.Println("Cast Crew Insertion started!!!")
	txn, err := db.Begin()
	CheckError(err)
	stmt, _ := txn.Prepare(pq.CopyIn("cast_crew", "pid", "tid", "category"))
	MapPid()
	MapTid()
	castCrews := GetAllCastCrew()
	for i := 0; i < len(castCrews); i++ {
		if Map_pid.Contains(castCrews[i].nconst) && Map_tid.Contains(castCrews[i].tconst) {
			_, err := stmt.Exec(castCrews[i].nconst, castCrews[i].tconst, castCrews[i].category)
			CheckError(err)
		}
	}
	_, err = stmt.Exec()
	CheckError(err)
	err = stmt.Close()
	CheckError(err)
	err = txn.Commit()
	CheckError(err)
	fmt.Println("Inserted Cast Crew successfully!!")
}
