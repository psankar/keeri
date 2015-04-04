// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import (
	"fmt"
	"strconv"
	"sync"
)

// All possible datatypes for the columns
type ColumnType int

const (
	IntColumn ColumnType = iota
	StringColumn
	CustomColumn
)

// Conveys the Name and the Type of any column
type ColumnDesc struct {
	ColName string
	ColType ColumnType
}

// maps column name to column-struct pointer
//
// The value in the map below will always
// be an instance of the column-struct
//
// We cannot use column-struct instead of interface{}
// below, because make(map[rowID]int/char) will
// fail to match the type map[string]column
type columnList map[string]interface{}

// Gets the column's value for the given rowID
type column map[rowID]interface{}

type table struct {

	// both the fields below will have to have
	// use a single instance of a readlock
	// named dataMetaDataLock
	cols     columnList
	colsDesc []ColumnDesc
	// As of now, this is a single table-level lock.
	// We will need more fine-grained locks later,
	// when we have to implement joins and also for
	// better performance.
	dataMetaDataLock sync.RWMutex

	rowCounter     rowID
	rowCounterLock sync.RWMutex
}

func (t *table) newRowID() rowID {
	// We could use UNIXTIME EPOCH as the rowid
	// instead of maintaining a counter, if there
	// are guarantees that the clock will not be changed
	// to a past time, after the db+process is started
	t.rowCounterLock.Lock()
	defer t.rowCounterLock.Unlock()

	t.rowCounter++
	return t.rowCounter
}

func (t *table) curRowID() rowID {
	t.rowCounterLock.RLock()
	defer t.rowCounterLock.RUnlock()
	return t.rowCounter
}

func (t *table) String() string {
	t.dataMetaDataLock.RLock()
	defer t.dataMetaDataLock.RUnlock()

	s := "\n"

	for _, j := range t.colsDesc {
		s += fmt.Sprintf("%s,", j.ColName)
	}
	s += "\n-------------------\n"

	for i := rowID(1); i <= t.curRowID(); i++ {
		s += fmt.Sprintf("%d) ", i)
		for _, j := range t.colsDesc {
			switch j.ColType {
			case IntColumn:
				col := t.cols[j.ColName].(map[rowID]int)
				s += strconv.Itoa(col[i])
			case StringColumn:
				col := t.cols[j.ColName].(map[rowID]string)
				s += col[i]
			case CustomColumn:
				col := t.cols[j.ColName].(map[rowID]interface{})
				s += fmt.Sprintf("%s", col[i])
			default:
			}
			s += ","
		}
		s += "\n"
	}

	s += "\nNumber of records: "
	s += strconv.Itoa(int(t.curRowID()))
	s += "\n"

	return s
}
