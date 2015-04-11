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
	"unicode"
	"unicode/utf8"
)

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

func parseQuery(sql string) (string, []string, *ConditionTree, error) {

	toks, err := tokenize(sql)
	if err != nil {
		return "", nil, nil, err
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
		return "", nil, nil, errors.New(fmt.Sprintf("Expected 'SELECT' Found '%s'", toks[pos]))
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
				return "", nil, nil, errors.New(fmt.Sprintf("Expected 'FROM' Found '%s'", toks[pos]))
			} else {
				pos++
				break
			}
		}
	}

	// Parse table names
	// TODO: A lot of changes are needed below to implement joins
	skipBlankTokens(toks, &pos)
	tableName = toks[pos]
	pos++

	skipBlankTokens(toks, &pos)
	if pos >= len(toks) {
		// Parsed until the end of the query
		return tableName, outCols, nil, nil
	}

	// Parsing the conditions
	if strings.ToUpper(toks[pos]) != "WHERE" {
		return "", nil, nil, errors.New(fmt.Sprintf("Expected 'WHERE' Found '%s'", toks[pos]))
	}

	var condTree *ConditionTree
	condTree, err = parseConditions(toks, pos)
	if err != nil {
		return "", nil, nil, err
	}

	return tableName, outCols, condTree, nil
}

func parseConditions(toks []string, pos int) (*ConditionTree, error) {

	// TODO: As of now, return nil which means all records will be returned
	return nil, nil

	t := &ConditionTree{}

	t.op = OR

	for {

		cond := Condition{}

		skipBlankTokens(toks, &pos)
		// lhsColName := toks[pos]
		pos++

		skipBlankTokens(toks, &pos)
		operator := toks[pos]

		switch operator {
		case "<":
			if toks[pos+1] == "=" {
				cond.op = LTE
				pos++
			} else {
				cond.op = LT
			}
		case ">":
			if toks[pos+1] == "=" {
				cond.op = GTE
				pos++
			} else {
				cond.op = GT
			}
		case "=":
			cond.op = EQ
		case "!":
			if toks[pos+1] != "=" {
				return nil, errors.New(fmt.Sprintf("Expected '!=' Found '%s'", toks[pos+1]))
			}
			cond.op = NEQ
		default:
			return nil, errors.New(fmt.Sprintf("Expected a Relational Operator, Found '%s'", toks[pos]))
		}

		pos++
		skipBlankTokens(toks, &pos)

		rhs := toks[pos]
		if rhs == "?" {
			// TODO: Get the yth parameter from the argv, y++
		}

		cond.value = rhs
		pos++

		t.conditions = append(t.conditions, cond)

		skipBlankTokens(toks, &pos)
		if pos > len(toks) {
			return t, nil
		}

		op := toks[pos]
		if op == "AND" {
			t.op = AND
		} else if op != "OR" {
			return nil, errors.New(fmt.Sprintf("Expected AND or OR, Found '%s'", op))
		}

		pos++
		skipBlankTokens(toks, &pos)
	}
}

func dummy() {
	input := []string{
		"(",
		"age",
		" ",
		">",
		" ",
		"18",
		" ",
		"AND",
		" ",
		"gender",
		" ",
		" ",
		"=",
		" ",
		" ",
		"f",
		" ",
		"OR",
		" ",
		" ",
		"status",
		" ",
		"=",
		"single",
		" ",
		"AND",
		" ",
		"(",
		" ",
		"col1",
		" ",
		"<",
		" ",
		"2",
		" ",
		"OR",
		" ",
		"col2",
		" ",
		">=",
		" ",
		"1",
		" ",
		")",
		" ",
		")"}

	fmt.Println(input)
}
