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

type table struct {
	cols     columnList
	colsDesc []ColumnDesc

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

	t.rowCounter += 1
	return t.rowCounter
}

func (t *table) curRowID() rowID {
	t.rowCounterLock.RLock()
	defer t.rowCounterLock.RUnlock()
	return t.rowCounter
}

func (t *table) String() string {
	s := "\n\n"

	for i, j := range t.colsDesc {
		s += fmt.Sprintf("%d) %s:%d\n", i, j.ColName, j.ColType)
	}

	s += "\nrowCounter: "
	s += strconv.Itoa(int(t.curRowID()))
	s += "\n"
	s += fmt.Sprintf("%v", t.cols)
	s += "\n\n"
	return s
}
