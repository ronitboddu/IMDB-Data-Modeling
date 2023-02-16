package config

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/ronitboddu/assignment1/pkg/properties"
)

const (
	host     = properties.Host
	port     = properties.Port
	user     = properties.User
	password = properties.Password
	dbname   = properties.Dbname
)

// Connects to the Database and returns the connection object
func GetConn() *sql.DB {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(0)
	err = db.Ping()
	CheckError(err)
	fmt.Println("Successful Connection!!")
	return db
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}
