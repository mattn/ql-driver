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

	//tx, err := conn.Begin()
	//if err != nil {
	//	log.Fatal(err)
	//}
	_, err = conn.Exec(`begin transaction;drop table foo;commit;`)
	_, err = conn.Exec(`begin transaction;create table foo(id int, value string);commit;`)
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Fatal(err)
	}
	for i := 1; i <= 100; i++ {
		_, err = conn.Exec(
			fmt.Sprintf(`begin transaction;insert into foo(id, value) values(%d, "test%03d");commit;`, i, i))
		if err != nil {
			log.Fatal(err)
		}
	}
	//tx.Commit()
	stmt, err := conn.Prepare(`select * from foo where id == $1`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	println("foo2")
	rows, err := stmt.Query(1)
	if err != nil {
		log.Fatal(err)
	}
	println("foo3")
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
