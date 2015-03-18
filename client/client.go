package main

import (
	"log"
	"time"

	"github.com/psankar/keeri/keeri"
)

func main() {
	db := &keeri.Keeri{}
	err := db.CreateTable("table1", keeri.ColumnDesc{ColName: "col1", ColType: keeri.IntColumn},
		keeri.ColumnDesc{ColName: "col2", ColType: keeri.StringColumn},
		keeri.ColumnDesc{ColName: "col3", ColType: keeri.CustomColumn})
	if err != nil {
		log.Fatal(err)
	}

	type point struct {
		x, y int
	}

	err = db.Insert("table1", 2, "Hello", point{0, 0})
	if err != nil {
		log.Fatal(err)
	}

	err = db.Insert("table1", 3, "World", time.Now())
	if err != nil {
		log.Fatal(err)
	}

	records := db.Select("table1")
	log.Println(records)
}
