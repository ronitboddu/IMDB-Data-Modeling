package service

import (
	"encoding/csv"
	"os"
)

//reads the data from tsv file and return the records.
func GetRecords(filename string) [][]string {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	csvReader := csv.NewReader(f)
	csvReader.LazyQuotes = true
	csvReader.Comma = '\t'
	csvReader.FieldsPerRecord = -1
	records, err := csvReader.ReadAll()
	if err != nil {
		panic(err)
	}
	return records
}
