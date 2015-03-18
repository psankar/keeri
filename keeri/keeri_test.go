package keeri

import (
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

	e := db.CreateTable("table1", ColumnDesc{ColName: "col1", ColType: IntColumn})
	if e != nil {
		t.Error("Test for single Int column creation failed", e)
	} else {
		t.Log("Test for single Int column creation")
	}
}

func TestInsertIntoTableWithSingleIntColumn(t *testing.T) {
	db := &Keeri{}

	e := db.CreateTable("table1", ColumnDesc{ColName: "col1", ColType: IntColumn})
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

	records := db.Select("table1")
	t.Log("Records returned:")
	t.Log(records)
}

func TestInsertIntoTableWithIntAndStringColumns(t *testing.T) {
	db := &Keeri{}

	e := db.CreateTable("table1", ColumnDesc{ColName: "col1", ColType: IntColumn},
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

	records := db.Select("table1")
	t.Log("Records returned:")
	t.Log(records)
}

func TestInsertIntoTableWithIntStringAndCustomColumns(t *testing.T) {
	db := &Keeri{}

	e := db.CreateTable("table1", ColumnDesc{ColName: "col1", ColType: IntColumn},
		ColumnDesc{ColName: "col2", ColType: StringColumn},
		ColumnDesc{ColName: "col3", ColType: CustomColumn})
	if e != nil {
		t.Error("Table creation failed", e)
	} else {
		t.Log("Table created")
	}

	type point struct {
		x, y int
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

	records := db.Select("table1")
	t.Log("Records returned:")
	t.Log(records)
}
