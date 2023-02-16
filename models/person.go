package models

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/lib/pq"
	"github.com/ronitboddu/assignment1/pkg/service"
)

// Object to Map each row from name.basics datafile
type Person struct {
	nconst      string
	primaryName string
	birthYear   string
	deathYear   string
}

var Persons []Person

const PERSON_FILENAME = "./clean_name_basics.tsv"

// Get all Persons
func GetAllPersons() []Person {
	if len(Persons) == 0 {
		records := service.GetRecords(PERSON_FILENAME)
		ExtractAllPersons(records, &Persons)
		return Persons
	}
	return Persons
}

// Extract all the persons records
func ExtractAllPersons(records [][]string, Persons *[]Person) {
	for i := 1; i < len(records); i++ {
		if len(records[i]) < 6 {
			fmt.Println("Found record length anamoly")
			continue
		}
		person := ExtractPerson(records[i])
		*Persons = append(*Persons, person)
	}
}

// Map individual row to an object
func ExtractPerson(record []string) Person {
	var person Person
	person.nconst = record[0]
	person.primaryName = record[1]
	person.birthYear = record[2]
	person.deathYear = record[3]
	return person
}

//Insert the data in the persons table
func InsertPersonsInDB(db *sql.DB) {
	fmt.Println("Person Insertion started!!!")
	persons := GetAllPersons()
	txn, err := db.Begin()
	CheckError(err)
	stmt, _ := txn.Prepare(pq.CopyIn("persons", "pid", "primary_name", "birth_year", "death_year"))
	for i := 0; i < len(persons); i++ {
		_, err := stmt.Exec(GetUniquePID(persons[i].nconst), persons[i].primaryName,
			persons[i].birthYear, persons[i].deathYear)
		CheckError(err)
	}
	_, err = stmt.Exec()
	CheckError(err)
	err = stmt.Close()
	CheckError(err)
	err = txn.Commit()
	CheckError(err)
	fmt.Println("Inserted Perons successfully!!")
}

//Extract unique integer value from the nconst attribute
func GetUniquePID(nconst string) int {
	id, err := strconv.Atoi(nconst[2:])
	CheckError(err)
	return id
}
