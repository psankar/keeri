package main

import (
	"fmt"

	"github.com/psankar/keeri/keeri"
)

func main() {
	db := &keeri.Keeri{}
	db.CreateTable("table1")
	s := "hello"
	db.Insert("table1", s)
	s = "world"
	fmt.Println(db.Select("table1"), s)
}
