// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/psankar/keeri/keeri"
)

type point struct {
	x, y int
}

func (p point) String() string {
	return fmt.Sprintf("Point{%d, %d}", p.x, p.y)
}

func main() {
	db := &keeri.Keeri{}
	err := db.CreateTable("table1",
		keeri.ColumnDesc{ColName: "col1", ColType: keeri.IntColumn},
		keeri.ColumnDesc{ColName: "col2", ColType: keeri.StringColumn},
		keeri.ColumnDesc{ColName: "col3", ColType: keeri.CustomColumn})
	if err != nil {
		log.Fatal(err)
	}

	err = db.Insert("table1", 18, "Hello", point{18, 18})
	if err != nil {
		log.Fatal(err)
	}

	err = db.Insert("table1", 143, "World", time.Now())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(db.String())
}
