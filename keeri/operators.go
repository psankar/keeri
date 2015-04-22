// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import "fmt"

type RelationalOperator int

const (
	EQ RelationalOperator = iota
	NEQ
	LT
	LTE
	GT
	GTE
)

type LogicalOperator int

const (
	OR LogicalOperator = iota
	AND
)

func (l LogicalOperator) String() string {
	switch l {
	case AND:
		return fmt.Sprint("AND")
	case OR:
		return fmt.Sprint("OR")
	default:
		panic("Unknown logical operator")
	}
}
