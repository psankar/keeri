// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

// TODO: JOINs are not supported. This struct will change.
import "fmt"

type Condition struct {
	op      RelationalOperator
	colDesc ColumnDesc
	colData interface{}

	// NOTE:
	// The below value could become an array of interfaces
	// to avoid repeated checks for same LHS for different RHS
	// when we implement support for Joins
	value interface{}
}

func (c Condition) String() string {
	ret := "\""
	ret += c.colDesc.ColName
	switch c.op {
	case LT:
		ret += "<"
	case LTE:
		ret += "<="
	case GT:
		ret += ">"
	case GTE:
		ret += ">="
	case EQ:
		ret += "="
	case NEQ:
		ret += "!="
	}
	ret += fmt.Sprintf("%v\"", c.value)
	return ret
}
