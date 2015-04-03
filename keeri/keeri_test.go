// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import (
	"fmt"
	"testing"
	"time"
)

func TestEmptyCreateTable(t *testing.T) {
	db := &Keeri{}
	e := db.CreateTable("table1")
	if e == nil {
		t.Error("Test for empty table creation failed")
	} else {
		t.Log("Test for empty table creation")
	}
}

func TestCreateTableWithSingleIntColumn(t *testing.T) {
	db := &Keeri{}

	e := db.CreateTable("table1",
		ColumnDesc{ColName: "col1", ColType: IntColumn})
	if e != nil {
		t.Error("Test for single Int column creation failed", e)
	} else {
		t.Log("Test for single Int column creation")
	}
}

func TestInsertIntoTableWithSingleIntColumn(t *testing.T) {
	db := &Keeri{}

	e := db.CreateTable("table1",
		ColumnDesc{ColName: "col1", ColType: IntColumn})
	if e != nil {
		t.Error("Test for single Int column creation, during single Int INSERT testing failed", e)
	} else {
		t.Log("Test for single Int column creation, during single Int INSERT testing")
	}

	e = db.Insert("table1", 2)
	if e != nil {
		t.Error("First INSERT failed")
	} else {
		t.Log("First INSERT")
	}

	e = db.Insert("table1", 3)
	if e != nil {
		t.Error("Second INSERT failed")
	} else {
		t.Log("Second INSERT")
	}

	t.Log(db.String())
}

func TestInsertIntoTableWithIntAndStringColumns(t *testing.T) {
	db := &Keeri{}

	e := db.CreateTable("table1",
		ColumnDesc{ColName: "col1", ColType: IntColumn},
		ColumnDesc{ColName: "col2", ColType: StringColumn})
	if e != nil {
		t.Error("Table creation failed", e)
	} else {
		t.Log("Table created")
	}

	e = db.Insert("table1", 2, "Hello")
	if e != nil {
		t.Error("First INSERT failed")
	} else {
		t.Log("First INSERT")
	}

	e = db.Insert("table1", 3, "World")
	if e != nil {
		t.Error("Second INSERT failed")
	} else {
		t.Log("Second INSERT")
	}

	e = db.Insert("table1", 4)
	if e != nil {
		t.Log("Insert with insufficient values failed correctly")
	} else {
		t.Error("No error message for insert with insufficient values")
	}

	t.Log(db.String())
}

type point struct {
	x, y int
}

func (p point) String() string {
	return fmt.Sprintf("Point{%d, %d}", p.x, p.y)
}

func TestInsertIntoTableWithIntStringAndCustomColumns(t *testing.T) {
	db := &Keeri{}

	e := db.CreateTable("table1",
		ColumnDesc{ColName: "col1", ColType: IntColumn},
		ColumnDesc{ColName: "col2", ColType: StringColumn},
		ColumnDesc{ColName: "col3", ColType: CustomColumn})
	if e != nil {
		t.Error("Table creation failed", e)
	} else {
		t.Log("Table created")
	}

	e = db.Insert("table1", 2, "Hello", point{0, 0})
	if e != nil {
		t.Error("First INSERT failed")
	} else {
		t.Log("First INSERT")
	}

	e = db.Insert("table1", 3, "World", time.Now())
	if e != nil {
		t.Error("Second INSERT failed")
	} else {
		t.Log("Second INSERT")
	}

	e = db.Insert("table1", 4)
	if e != nil {
		t.Log("Insert with insufficient values failed correctly")
	} else {
		t.Error("No error message for insert with insufficient values")
	}

	e = db.Insert("table1", "blah", "blah", "blah")
	if e != nil {
		t.Log("Insert with mismatched types failed correctly:\n", e)
	} else {
		t.Error("No error message for insert with mismatched types")
	}

	t.Log(db.String())
}
