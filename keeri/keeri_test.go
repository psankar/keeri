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

func TestCondition(t *testing.T) {
	db := &Keeri{}

	_ = db.CreateTable("table1",
		ColumnDesc{ColName: "col1", ColType: IntColumn},
		ColumnDesc{ColName: "col2", ColType: StringColumn},
		ColumnDesc{ColName: "col3", ColType: CustomColumn})

	_ = db.Insert("table1", 1, "STRDATA1", time.Now())
	_ = db.Insert("table1", 2, "STRDATA2", point{0, 0})
	_ = db.Insert("table1", 3, "STRDATA2", point{1, 1})
	_ = db.Insert("table1", 3000, "STRDATA2", time.Now())
	t.Log(db.String())

	t.Log("SELECT col1, col2 FROM table1 WHERE col2='STRDATA1'")
	c1 := &ConditionTree{
		op: OR,
		conditions: []Condition{
			Condition{
				op:      EQ,
				colType: StringColumn,
				colData: db.tables["table1"].cols["col2"].(map[rowID]string),
				value:   "STRDATA1",
			},
		},
		children: nil,
	}

	res, err := db.Query("table1", []string{"col1", "col2"}, c1)
	if err != nil {
		t.Error(err)
	}
	t.Log(res)

	t.Log("SELECT col1, col2, col3 FROM table1 WHERE col2='STRDATA2' AND col1 > 1000")
	c2 := &ConditionTree{
		op: AND,
		conditions: []Condition{
			Condition{
				op:      EQ,
				colType: StringColumn,
				colData: db.tables["table1"].cols["col2"].(map[rowID]string),
				value:   "STRDATA2",
			},
			Condition{
				op:      LT,
				colType: IntColumn,
				colData: db.tables["table1"].cols["col1"].(map[rowID]int),
				value:   1000,
			},
		},
		children: nil,
	}

	res, err = db.Query("table1", []string{"col1", "col2", "col3"}, c2)
	if err != nil {
		t.Error(err)
	}
	t.Log(res)
}
