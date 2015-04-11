// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
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

type RelationalOperator int

const (
	EQ RelationalOperator = iota
	NEQ
	LT
	LTE
	GT
	GTE
)

// TODO: JOINs are not supported. This struct will change.
type Condition struct {
	op      RelationalOperator
	colType ColumnType
	colData interface{}

	// NOTE:
	// The below value could become an array of interfaces
	// to avoid repeated checks for same LHS for different RHS
	// when we implement support for Joins
	value interface{}
}

type LogicalOperator int

const (
	OR LogicalOperator = iota
	AND
)

type ConditionTree struct {
	op         LogicalOperator
	conditions []Condition

	children []*ConditionTree
}

// Evaluate the conditions recursively and return the rowIDs
// that match all the conditions recursively. Not threadsafe.
//
// Locks should be handled by the caller, as any panic in this
// recursion should not cause any dangling, stale-locked locks.
// Not threadsafe. Caller should have acquired readlock
func (t *ConditionTree) evaluate() []rowID {

	var wg sync.WaitGroup

	chiRowIDs := make([]([]rowID), len(t.children))
	for i := 0; i < len(t.children); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l := t.children[i].evaluate()
			chiRowIDs[i] = append(chiRowIDs[i], l...)
		}(i)
	}

	conRowIDs := make([]([]rowID), len(t.conditions))
	for i, c := range t.conditions {
		wg.Add(1)
		go func(i int, c Condition) {
			defer wg.Done()
			l := evaluateCondition(&c)
			conRowIDs[i] = append(conRowIDs[i], l...)
		}(i, c)
	}

	wg.Wait()

	var ret []rowID
	if t.op == OR {
		var rows []rowID
		for _, v := range chiRowIDs {
			rows = append(rows, v...)
		}

		for _, v := range conRowIDs {
			rows = append(rows, v...)
		}
		ret = sortAndDeDup(rows)
	} else if t.op == AND {

		// Find the rowIDs that exist in all the
		// individual sets of chiRowIDs and conRowIDs
		var unifiedRowIDs []([]rowID)
		for _, v := range chiRowIDs {
			unifiedRowIDs = append(unifiedRowIDs, v)
		}
		for _, v := range conRowIDs {
			unifiedRowIDs = append(unifiedRowIDs, v)
		}

		foundMap := make(map[rowID]bool)
		notFoundMap := make(map[rowID]bool)

		for i, curArr := range unifiedRowIDs {
			for _, el := range curArr {

				if foundMap[el] == true {
					continue
				}

				if notFoundMap[el] == true {
					continue
				}

				foundCount := 0

			skipEl:
				for j, cmpArr := range unifiedRowIDs {
					if j != i {
						for _, k := range cmpArr {
							// TODO: We need to implement a rowIDCmp function
							// that works similar to strcmp but on the rowID
							// datatype. The == and > below will for now,
							// as long as the rowID is of type int
							if k == el {
								foundCount++
								break
							} else if k > el {
								// el should be added to the notFoundMap
								//
								// 'break label' is used instead of goto,
								// as the code to add to map below is inside
								// an else block and so goto won't work.
								break skipEl
							}
						}
					}
				}

				if foundCount == len(unifiedRowIDs)-1 {
					foundMap[el] = true
				} else {
					// Will come here from the "break skipEl" above
					// And also when we have a number that is bigger than
					// all the other elements in all the other arrays
					notFoundMap[el] = true
				}
			}
		}

		rows := make([]rowID, 0, len(foundMap))
		for k, _ := range foundMap {
			rows = append(rows, k)
		}
		ret = sortAndDeDup(rows)
	} else {
		panic("Not reachable")
	}

	return ret
}

// Not threadsafe. Caller should have acquired readlock
func evaluateCondition(i *Condition) []rowID {
	var ret []rowID

	switch i.colType {
	case IntColumn:
		switch i.op {
		case EQ:
			for k, v := range i.colData.(map[rowID]int) {
				if v == i.value.(int) {
					ret = append(ret, k)
				}
			}
		case NEQ:
			for k, v := range i.colData.(map[rowID]int) {
				if v != i.value.(int) {
					ret = append(ret, k)
				}
			}
		case LT:
			for k, v := range i.colData.(map[rowID]int) {
				if v < i.value.(int) {
					ret = append(ret, k)
				}
			}
		case LTE:
			for k, v := range i.colData.(map[rowID]int) {
				if v <= i.value.(int) {
					ret = append(ret, k)
				}
			}
		case GT:
			for k, v := range i.colData.(map[rowID]int) {
				if v > i.value.(int) {
					ret = append(ret, k)
				}
			}
		case GTE:
			for k, v := range i.colData.(map[rowID]int) {
				if v >= i.value.(int) {
					ret = append(ret, k)
				}
			}
		default:
			panic("Unsupported relational operation for int")
		}
	case StringColumn:
		switch i.op {
		case EQ:
			//TODO: Implement wildcard support
			for k, v := range i.colData.(map[rowID]string) {
				if v == i.value.(string) {
					ret = append(ret, k)
				}
			}
		case NEQ:
			for k, v := range i.colData.(map[rowID]string) {
				if v != i.value.(string) {
					ret = append(ret, k)
				}
			}
		default:
			panic("Unsupported relational operation for string")
		}
	case CustomColumn:
	default:
		panic("Unsupported column type ")
	}

	return ret
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
			return nil, errors.New(fmt.Sprintf("Invalid column name", outColName))
		}
	}

	tbl.dataMetaDataLock.RLock()
	defer tbl.dataMetaDataLock.RUnlock()

	matchingRowIDs := cTree.evaluate()

	var results []interface{}
	for _, rID := range matchingRowIDs {
		var row []interface{}
		for _, i := range resultsDesc {
			switch i.colType {
			case IntColumn:
				field, ok := (i.mapPointer.(map[rowID]int))[rID]
				if ok != true {
					return nil, errors.New(
						fmt.Sprintf("Data corruption. No data found for rowID [%v] in a column", rID))
				}
				row = append(row, field)
			case StringColumn:
				field, ok := (i.mapPointer.(map[rowID]string))[rID]
				if ok != true {
					return nil, errors.New(
						fmt.Sprintf("Data corruption. No data found for rowID [%v] in a column", rID))
				}
				row = append(row, field)
			case CustomColumn:
				field, ok := (i.mapPointer.(map[rowID]interface{}))[rID]
				if ok != true {
					return nil, errors.New(
						fmt.Sprintf("Data corruption. No data found for rowID [%v] in a column", rID))
				}
				row = append(row, field)
			}
		}
		results = append(results, row)
	}

	return results, nil
}

func skipBlankTokens(toks []string, pos *int) {
	for {
		if *pos >= len(toks) {
			return
		}
		r, _ := utf8.DecodeRuneInString(toks[*pos])
		if unicode.IsSpace(r) {
			*pos++
		} else {
			break
		}
	}
}

func (db *Keeri) Select(sql string) ([]interface{}, error) {
	toks, err := tokenize(sql)
	if err != nil {
		return nil, err
	}

	fmt.Println()
	for _, i := range toks {
		fmt.Printf("[%v]", i)
	}
	fmt.Println()

	pos := 0
	var outCols []string
	var tableName string

	// Trim any blanks in the prefix of the query
	skipBlankTokens(toks, &pos)

	if strings.ToUpper(toks[pos]) != "SELECT" {
		return nil, errors.New(fmt.Sprintf("Expected 'SELECT' Found '%s'", toks[pos]))
	}

	// Parse (comma sepearated column names) or (a single column name)
	// by parsing until the FROM keyword
	pos++
	for {

		// Trim any blanks
		skipBlankTokens(toks, &pos)

		// TODO: Check if valid column name
		outCols = append(outCols, toks[pos])
		pos++

		// Trim any blanks
		skipBlankTokens(toks, &pos)

		if toks[pos] == "," {
			// More than one column needs to be output for this query
			pos++
			continue
		} else {
			skipBlankTokens(toks, &pos)
			if strings.ToUpper(toks[pos]) != "FROM" {
				return nil, errors.New(fmt.Sprintf("Expected 'FROM' Found '%s'", toks[pos]))
			} else {
				pos++
				break
			}
		}
	}

	// Parse table names
	// TODO: A lot of changes are needed below to implement joins
	pos++

	skipBlankTokens(toks, &pos)
	tableName = toks[pos]
	pos++

	skipBlankTokens(toks, &pos)

	if pos >= len(toks) {
		// Parsed until the end of the query
		goto fetchRecords
	}

	// Parsing the conditions
	if strings.ToUpper(toks[pos]) != "WHERE" {
		return nil, errors.New(fmt.Sprintf("Expected WHERE Found %s", toks[pos]))
	}

	// TODO: Parse the individual constraints and build the tree

fetchRecords:

	fmt.Println("table name:", tableName, "out columns:", outCols)

	return nil, nil
}
