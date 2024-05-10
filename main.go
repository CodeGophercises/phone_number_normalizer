package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"unicode"

	_ "github.com/lib/pq"
)

var debug = flag.Bool("debug", false, "debug mode")
var insertFlag = flag.String("insert", "", "the file to insert data")

// TODO: Take them from configuration
const (
	user   = "jatinmalik"
	dbname = "gotest"
)

func normalize_phone(phone string) string {
	p := []rune(phone)
	var res []rune
	for _, b := range p {
		if unicode.IsDigit(b) {
			// Append in result
			res = append(res, b)
		}
	}
	return string(res)
}

func wait() {
	if *debug == false {
		return
	}
	var input string
	fmt.Printf(":> Press Enter to continue")
	fmt.Scanln(&input)
}

func insertData(db *sql.DB) {
	dataFile := *insertFlag
	if dataFile == "" {
		return
	}
	f, err := os.Open(dataFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	insertSql := `
	Insert into phone_numbers values ($1)
	`
	for scanner.Scan() {
		_, err := db.Exec(insertSql, scanner.Text())
		if err != nil {
			panic(err)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

}

func connectDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	log.Printf("Connection established.\n")
	return db, nil
}
func main() {
	flag.Parse()
	if *debug == false {
		log.SetOutput(io.Discard)
	}
	// Let's connect to our postgres server.

	dsn := fmt.Sprintf("user=%s dbname=%s sslmode=disable", user, dbname)
	db, err := connectDB(dsn)
	if err != nil {
		panic(err)
	}
	insertData(db)
	wait()

	// Iterate through all entries in table and normalize them

	selectSql := `select * from phone_numbers`
	rows, err := db.Query(selectSql)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	uniquePhones := make(map[string]struct{})

	updateSql := `update phone_numbers set phone = $1 where phone = $2`
	for rows.Next() {
		var phone string
		rows.Scan(&phone)
		log.Println("Normalizing entry:", phone)
		wait()
		nPhone := normalize_phone(phone)
		_, exists := uniquePhones[nPhone]
		if exists {
			log.Println("Deleting old entry:", nPhone)
			// delete old entry
			_, err := db.Exec("delete from phone_numbers where phone=$1", nPhone)
			if err != nil {
				panic(err)
			}
		}
		wait()
		if phone == nPhone {
			if exists {
				// Insert
				log.Println("Inserting new entry as all previous ones got deleted")
				_, err := db.Exec("Insert into phone_numbers values ($1)", phone)
				if err != nil {
					panic(err)
				}
			}

		} else {
			log.Println("Updating entry")
			_, err := db.Exec(updateSql, nPhone, phone)
			if err != nil {
				panic(err)
			}
		}
		uniquePhones[nPhone] = struct{}{}
		wait()
	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

}
