package keeri

type Keeri struct {
	// maps tablename to table pointer
	tables map[string]table
}

// maps column name to column pointer
type table map[string]column

// maps rowId to the location where data is saved
type column map[rowID]interface{}

// uniquely identifies a row
type rowID int

func (db *Keeri) CreateTable(tableName string) {

	t := make(map[string]column)
	t["col1"] = make(map[rowID]interface{})
	if db.tables == nil {
		db.tables = make(map[string]table)
	}
	db.tables[tableName] = t
}

func (db *Keeri) Insert(tableName string, value interface{}) {
	db.tables[tableName]["col1"][1] = value
}

func (db *Keeri) Select(tableName string) interface{} {
	return db.tables[tableName]["col1"][1]
}
