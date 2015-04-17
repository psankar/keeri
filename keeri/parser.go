// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import (
	"errors"
	"fmt"
	"log"
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

	_ = removeRelOpsGenerateSQLToks(words[pos+1:])
	// TODO: Grab the tokens generated above and generate a single
	// condition tree that encompasses everything correctly

	return tableName, outCols, nil, nil
}

type sqlTokType int

const (
	LEFT_PARAN_TOK sqlTokType = iota
	RIGHT_PARAN_TOK
	CONDITION_PTR_TOK
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
	case AND_TOK:
		return fmt.Sprint("AND")
	case OR_TOK:
		return fmt.Sprint("OR")
	}
	panic("Unreachable")
}

// NOTE: Caller must set the op field of the condition,
// and that is why this is called a 'Partial' function
func createPartialCondTok(words []string, lhsPos, pos *int) *sqlTokens {
	if *lhsPos == -1 {
		panic(fmt.Errorf("No operand found for operator at '%s' ", words[*pos]))
	}

	*pos++
	skipEmptyWords(words, pos)

	cond := &Condition{
		op: LT,
		colDesc: ColumnDesc{
			ColName: words[*lhsPos],
			ColType: unRecognizedColumn,
		},
		value: words[*pos],
	}

	tok := &sqlTokens{
		CONDITION_PTR_TOK,
		cond,
	}
	*lhsPos = -1

	return tok
}

// This function removes the relational opera[tors|nds]
// in the incoming sql words, generates an
// array of tokens where each relational operator
// and the operands would have been replaced with
// a condition object
func removeRelOpsGenerateSQLToks(words []string) (ret []sqlTokens) {

	lhsPos := -1

	for i := 0; i < len(words); i++ {

		switch words[i] {
		case "(":
			ret = append(ret, sqlTokens{LEFT_PARAN_TOK, nil})
		case ")":
			ret = append(ret, sqlTokens{RIGHT_PARAN_TOK, nil})
		case "AND":
			ret = append(ret, sqlTokens{AND_TOK, nil})
		case "OR":
			ret = append(ret, sqlTokens{OR_TOK, nil})

		// Relational Operators
		case "<":
			tok := createPartialCondTok(words, &lhsPos, &i)
			cond := tok.value.(*Condition)
			cond.op = LT
			ret = append(ret, *tok)
		case "<=":
			tok := createPartialCondTok(words, &lhsPos, &i)
			cond := tok.value.(*Condition)
			cond.op = LTE
			ret = append(ret, *tok)
		case ">":
			tok := createPartialCondTok(words, &lhsPos, &i)
			cond := tok.value.(*Condition)
			cond.op = GT
			ret = append(ret, *tok)
		case ">=":
			tok := createPartialCondTok(words, &lhsPos, &i)
			cond := tok.value.(*Condition)
			cond.op = GTE
			ret = append(ret, *tok)
		case "=":
			tok := createPartialCondTok(words, &lhsPos, &i)
			cond := tok.value.(*Condition)
			cond.op = EQ
			ret = append(ret, *tok)
		case "!=":
			tok := createPartialCondTok(words, &lhsPos, &i)
			cond := tok.value.(*Condition)
			cond.op = NEQ
			ret = append(ret, *tok)

		case " ":
			//Do nothing

		default:
			// column name operand for a relational operator
			if lhsPos != -1 {
				panic(fmt.Errorf("Invalid tokens: %s %s", words[lhsPos], words[i]))
			}

			lhsPos = i
		}
	}

	if lhsPos != -1 {
		panic(fmt.Errorf("Invalid token: %s", words[lhsPos]))
	}

	log.Printf("The sqltokens from the sql words are as follows:\n%v\n\n", ret)

	return
}
