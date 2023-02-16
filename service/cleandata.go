package service

import (
	"io/ioutil"
	"os"
	"regexp"

	"github.com/ronitboddu/assignment1/pkg/properties"
)

// Function to clean the tsv file by removing double quotes from the file
func dataCleaning(clean_file_path string, og_file_path string) {
	if _, err := os.Stat(clean_file_path); err != nil {
		content, err := ioutil.ReadFile(og_file_path)
		CheckError(err)
		s := string(content)
		re := regexp.MustCompile(`"`)
		new_data := re.ReplaceAllString(s, " ")
		f, err := os.Create(clean_file_path)
		CheckError(err)
		defer f.Close()
		_, err1 := f.WriteString(new_data)
		CheckError(err1)
	}
}

func CleanData() {
	clean_file_path := "./clean_name_basics.tsv"
	og_file_path := properties.NAME_BASICS
	dataCleaning(clean_file_path, og_file_path)
	clean_file_path = "./clean_title_basics.tsv"
	og_file_path = properties.TITLE_BASICS_TSV
	dataCleaning(clean_file_path, og_file_path)
	clean_file_path = "./clean_title_principals.tsv"
	og_file_path = properties.TITLE_PRINCIPALS
	dataCleaning(clean_file_path, og_file_path)
	clean_file_path = "./clean_title_ratings.tsv"
	og_file_path = properties.TITLE_RATINGS_TSV
	dataCleaning(clean_file_path, og_file_path)
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}
