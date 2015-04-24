// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
)

// The database descriptor that could hold any number of tables
type Keeri struct {
	tables map[string]*table

	tblNamesLock sync.RWMutex
}

// Creates a new table, with one or more columns
func (db *Keeri) CreateTable(tableName string, cols ...ColumnDesc) error {

	db.tblNamesLock.Lock()
	defer db.tblNamesLock.Unlock()

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

	tbl.dataMetaDataLock.Lock()
	defer tbl.dataMetaDataLock.Unlock()

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

func (db *Keeri) String() interface{} {

	// As of now, returning all the records as a tabulated CSV string
	row := ""

	db.tblNamesLock.RLock()
	defer db.tblNamesLock.RUnlock()

	for tblName, tbl := range db.tables {
		row += fmt.Sprintf("\n%s%s", tblName, tbl)
	}

	return row
}

func (db *Keeri) Query(tableName string, colNames []string,
	cTree *ConditionTree) ([]interface{}, error) {

	db.tblNamesLock.RLock()
	tbl := db.tables[tableName]
	db.tblNamesLock.RUnlock()
	if tbl == nil {
		return nil, errors.New("Table not found")
	}

	type resultsColsDesc struct {
		colType    ColumnType
		mapPointer interface{}
	}

	var resultsDesc []resultsColsDesc
	// Validate asked column names and get their data pointers
	for _, outColName := range colNames {
		found := false
		for _, i := range tbl.colsDesc {
			if i.ColName == outColName {
				found = true
				resultsDesc = append(resultsDesc,
					resultsColsDesc{
						colType:    i.ColType,
						mapPointer: tbl.cols[outColName],
					})
				break
			}
		}
		if found != true {
			return nil, fmt.Errorf("Invalid column name", outColName)
		}
	}

	tbl.dataMetaDataLock.RLock()
	defer tbl.dataMetaDataLock.RUnlock()

	var matchingRowIDs []rowID
	if cTree != nil {
		matchingRowIDs = cTree.evaluate()
	}

	var results []interface{}
	for _, rID := range matchingRowIDs {
		var row []interface{}
		for _, i := range resultsDesc {
			switch i.colType {
			case IntColumn:
				field, ok := (i.mapPointer.(map[rowID]int))[rID]
				if ok != true {
					return nil,
						fmt.Errorf("Data corruption. No data found for rowID [%v] in a column", rID)
				}
				row = append(row, field)
			case StringColumn:
				field, ok := (i.mapPointer.(map[rowID]string))[rID]
				if ok != true {
					return nil,
						fmt.Errorf("Data corruption. No data found for rowID [%v] in a column", rID)
				}
				row = append(row, field)
			case CustomColumn:
				field, ok := (i.mapPointer.(map[rowID]interface{}))[rID]
				if ok != true {
					return nil,
						fmt.Errorf("Data corruption. No data found for rowID [%v] in a column", rID)
				}
				row = append(row, field)
			}
		}
		results = append(results, row)
	}

	return results, nil
}

func (db *Keeri) Select(sql string, args ...interface{}) (ret []interface{}, err error) {

	defer func() {
		// TODO: Atomicity yet to be implemented.
		// Partial inserts will exist in case of errors
		if r := recover(); r != nil {
			ret = nil
			err = r.(error)
		}
	}()

	tblName, cols, condTree := parseQuery(sql)

	if condTree != nil {
		buf := new(bytes.Buffer)
		err = json.Indent(buf, []byte(condTree.String()), "", "  ")
		if err != nil {
			return nil, errors.New("Error converting condTree into a JSON string")
		}
		log.Println(buf)
	}

	db.tblNamesLock.RLock()
	tbl := db.tables[tblName]
	db.tblNamesLock.RUnlock()

	if tbl == nil {
		return nil, fmt.Errorf("Invalid table name '%s'", tblName)
	}

	if condTree != nil {
		tbl.dataMetaDataLock.RLock()
		resolveColDetails(tbl, condTree)
		tbl.dataMetaDataLock.RUnlock()
	}

	ret, err = db.Query(tblName, cols, condTree)
	return
}

func resolveColDetails(tbl *table, i *ConditionTree) {
	for _, j := range i.conditions {
		colName := j.colDesc.ColName
		for _, k := range tbl.colsDesc {
			if k.ColName == colName {
				j.colDesc.ColType = k.ColType
				j.colData = tbl.cols[colName]

				switch k.ColType {
				case StringColumn: // Do Nothing
				case IntColumn:
					t, e := strconv.Atoi(j.value.(string))
					if e != nil {
						panic(e)
					}
					j.value = t
				default:
					panic("Unsupported column type")
				}
			}
		}

		if j.colDesc.ColType == unRecognizedColumn {
			panic(fmt.Errorf("Invalid column name: %s", colName))
		}
	}

	for _, i := range i.children {
		resolveColDetails(tbl, i)
	}
}
