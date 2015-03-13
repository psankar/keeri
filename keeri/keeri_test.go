package keeri

import "testing"

func TestCreateTable(t *testing.T) {
	db := &Keeri{}
	db.CreateTable("table1")
}
