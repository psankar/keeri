// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import (
	"errors"
	"fmt"
	"strconv"
)

// The database descriptor that could hold any number of tables
type Keeri struct {
	tables map[string]*table
}

// maps column name to column pointer
//
// The value int the map below will always
// be an instance of column.
//
// We cannot use column instead of interface{}
// below, because make(map[rowID]int/char) will
// fail to match the type map[string]column
type columnList map[string]interface{}

// Gets the column's value for the given rowID
type column map[rowID]interface{}

// uniquely identifies a row
type rowID uint

// Creates a new table, with one or more columns
func (db *Keeri) CreateTable(tableName string, cols ...ColumnDesc) error {

	if len(cols) < 1 {
		return errors.New("Empty table")
	}

	dbCols := make(map[string]interface{})
	for _, col := range cols {
		switch col.ColType {
		case IntColumn:
			dbCols[col.ColName] = make(map[rowID]int)
		case StringColumn:
			dbCols[col.ColName] = make(map[rowID]string)
		case CustomColumn:
			dbCols[col.ColName] = make(map[rowID]interface{})
		default:
			return errors.New("Invalid column type specified")
		}

	}

	if db.tables == nil {
		db.tables = make(map[string]*table)
	}

	t := &table{
		cols:       dbCols,
		colsDesc:   cols,
		rowCounter: rowID(0),
	}

	db.tables[tableName] = t
	return nil
}

func (db *Keeri) Insert(tableName string, values ...interface{}) (err error) {
	tbl := db.tables[tableName]

	if len(tbl.colsDesc) != len(values) {
		return errors.New("Column count mismatch")
	}

	id := tbl.newRowID()

	defer func() {
		// TODO: Atomicity yet to be implemented.
		// Partial inserts will exist in case of errors
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	for i, j := range tbl.colsDesc {
		switch j.ColType {
		case IntColumn:
			col := tbl.cols[j.ColName].(map[rowID]int)
			col[id] = values[i].(int)
		case StringColumn:
			col := tbl.cols[j.ColName].(map[rowID]string)
			col[id] = values[i].(string)
		case CustomColumn:
			col := tbl.cols[j.ColName].(map[rowID]interface{})
			col[id] = values[i]
		}
	}

	return nil
}

func (db *Keeri) Select(tableName string) interface{} {

	// As of now, returning all the records as a tabulated CSV string
	row := "\n"
	tbl := db.tables[tableName]
	for i := rowID(1); i <= tbl.curRowID(); i++ {
		for _, j := range tbl.colsDesc {
			switch j.ColType {
			case IntColumn:
				col := tbl.cols[j.ColName].(map[rowID]int)
				row += strconv.Itoa(col[i])
			case StringColumn:
				col := tbl.cols[j.ColName].(map[rowID]string)
				row += col[i]
			case CustomColumn:
				col := tbl.cols[j.ColName].(map[rowID]interface{})
				row += fmt.Sprintf("%s", col[i])
			default:
			}
			row += ","
		}
		row += "\n"
	}

	return row
}
