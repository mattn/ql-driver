package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/ql-driver"
	"log"
)

func main() {
	conn, err := sql.Open("ql", "foo.db")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Exec(`drop table foo`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = conn.Exec(`create table foo(id int, value string)`)
	if err != nil {
		log.Fatal(err)
	}
	for i := 1; i <= 100; i++ {
		_, err = conn.Exec(
			`insert into foo(id, value) values($1, $2)`, i, fmt.Sprintf("test%03d", i))
		if err != nil {
			log.Fatal(err)
		}
	}
	stmt, err := conn.Prepare(`select * from foo where id == $1`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(1)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var id int
		var value string
		err = rows.Scan(&id, &value)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, value)
	}
}
