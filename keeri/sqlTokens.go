// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import "fmt"

type sqlTokType int

const (
	LEFT_PARAN_TOK sqlTokType = iota
	RIGHT_PARAN_TOK
	CONDITION_PTR_TOK
	CONDITION_TREE_PTR_TOK
	AND_TOK
	OR_TOK
)

type sqlTokens struct {
	tokType sqlTokType
	value   interface{}
}

func (t sqlTokens) String() string {
	switch t.tokType {
	case LEFT_PARAN_TOK:
		return fmt.Sprint("(")
	case RIGHT_PARAN_TOK:
		return fmt.Sprint(")")
	case CONDITION_PTR_TOK:
		return fmt.Sprintf("%s", t.value.(*Condition))
	case CONDITION_TREE_PTR_TOK:
		return fmt.Sprint("ConditionTree")
	case AND_TOK:
		return fmt.Sprint("AND")
	case OR_TOK:
		return fmt.Sprint("OR")
	}
	panic("Unreachable")
}
