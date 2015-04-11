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
)

func skipEmptyWords(words []string, pos *int) {
	for {
		if *pos >= len(words) {
			return
		}

		// This will work as long as the \n \r etc.
		// on the input SQL string are replaced with
		// a normal blank space, in the scanner's
		// scanSQLWords function. If not we will have to
		// use unicode.IsSpace here. Refer to the history
		// of this function.
		if words[*pos] == " " {
			*pos++
		} else {
			break
		}
	}
}

func parseQuery(sql string) (string, []string, *ConditionTree, error) {

	words, err := splitSQL(sql)
	if err != nil {
		return "", nil, nil, err
	}

	fmt.Println()
	for _, i := range words {
		fmt.Printf("[%v]", i)
	}
	fmt.Println()

	pos := 0
	var outCols []string
	var tableName string

	// Trim any blanks in the prefix of the query
	skipEmptyWords(words, &pos)

	if strings.ToUpper(words[pos]) != "SELECT" {
		return "", nil, nil, errors.New(fmt.Sprintf("Expected 'SELECT' Found '%s'", words[pos]))
	}

	// Parse (comma sepearated column names) or (a single column name)
	// by parsing until the FROM keyword
	pos++
	for {

		// Trim any blanks
		skipEmptyWords(words, &pos)

		// TODO: Check if valid column name
		outCols = append(outCols, words[pos])
		pos++

		// Trim any blanks
		skipEmptyWords(words, &pos)

		if words[pos] == "," {
			// More than one column needs to be output for this query
			pos++
			continue
		} else {
			skipEmptyWords(words, &pos)
			if strings.ToUpper(words[pos]) != "FROM" {
				return "", nil, nil, errors.New(fmt.Sprintf("Expected 'FROM' Found '%s'", words[pos]))
			} else {
				pos++
				break
			}
		}
	}

	// Parse table names
	// TODO: A lot of changes are needed below to implement joins
	skipEmptyWords(words, &pos)
	tableName = words[pos]
	pos++

	skipEmptyWords(words, &pos)
	if pos >= len(words) {
		// Parsed until the end of the query
		return tableName, outCols, nil, nil
	}

	// Parsing the conditions
	if strings.ToUpper(words[pos]) != "WHERE" {
		return "", nil, nil, errors.New(fmt.Sprintf("Expected 'WHERE' Found '%s'", words[pos]))
	}

	var condTree *ConditionTree
	condTree, err = parseConditions(words, pos)
	if err != nil {
		return "", nil, nil, err
	}

	return tableName, outCols, condTree, nil
}

func parseConditions(words []string, pos int) (*ConditionTree, error) {

	// TODO: As of now, return nil which means all records will be returned
	return nil, nil

	t := &ConditionTree{}

	t.op = OR

	for {

		cond := Condition{}

		skipEmptyWords(words, &pos)
		// lhsColName := words[pos]
		pos++

		skipEmptyWords(words, &pos)
		operator := words[pos]

		switch operator {
		case "<":
			if words[pos+1] == "=" {
				cond.op = LTE
				pos++
			} else {
				cond.op = LT
			}
		case ">":
			if words[pos+1] == "=" {
				cond.op = GTE
				pos++
			} else {
				cond.op = GT
			}
		case "=":
			cond.op = EQ
		case "!":
			if words[pos+1] != "=" {
				return nil, errors.New(fmt.Sprintf("Expected '!=' Found '%s'", words[pos+1]))
			}
			cond.op = NEQ
		default:
			return nil, errors.New(fmt.Sprintf("Expected a Relational Operator, Found '%s'", words[pos]))
		}

		pos++
		skipEmptyWords(words, &pos)

		rhs := words[pos]
		if rhs == "?" {
			// TODO: Get the yth parameter from the argv, y++
		}

		cond.value = rhs
		pos++

		t.conditions = append(t.conditions, cond)

		skipEmptyWords(words, &pos)
		if pos > len(words) {
			return t, nil
		}

		op := words[pos]
		if op == "AND" {
			t.op = AND
		} else if op != "OR" {
			return nil, errors.New(fmt.Sprintf("Expected AND or OR, Found '%s'", op))
		}

		pos++
		skipEmptyWords(words, &pos)
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
