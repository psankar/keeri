// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import (
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

// This function takes an incoming SQL string and creates a condition tree
// out of the WHERE clause nested conditions. However, the conditions will
// have just the column names resolved but not the column types. The caller
// (parser) should take care of filling the column types in the condTree
// that is returned, before using it in an eval function.
// TODO: Probably a good idea to add a 'state' in the CondTree struct, which
// could be updated after colTypes are resolved and checked in evaluate func
func parseQuery(sql string) (string, []string, *ConditionTree) {

	words, err := splitSQL(sql)
	if err != nil {
		panic(err)
	}

	pos := 0
	var outCols []string
	var tableName string

	// Trim any blanks in the prefix of the query
	skipEmptyWords(words, &pos)

	if strings.ToUpper(words[pos]) != "SELECT" {
		panic(fmt.Errorf("Expected 'SELECT' Found '%s'", words[pos]))
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
				panic(fmt.Errorf("Expected 'FROM' Found '%s'", words[pos]))
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
		return tableName, outCols, nil
	}

	// Parsing the conditions
	if strings.ToUpper(words[pos]) != "WHERE" {
		panic(fmt.Errorf("Expected 'WHERE' Found '%s'", words[pos]))
	}

	toks := removeRelOpsGenerateSQLToks(words[pos+1:])
	cTree := generateCondTree(toks, 0, len(toks)-1, 0)

	return tableName, outCols, cTree
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

func generateCondTree(toks []sqlTokens, lo, hi, recursionLevel int) *ConditionTree {

	log.Println(recursionLevel, "Entering generateCondTree with", lo, hi, toks[lo:hi])

	// Convert all expressions within parantheses into
	// ConditionTrees and update the toks
	//
	// At this stage, toks can have Conditions, ConditionTrees,
	// Parantheses and LogicalOperators; But no relational operators
	for {
		openParLoc := -1
		parenFound := false

		for pos := lo; pos <= hi; pos++ {
			log.Println(recursionLevel, "Evaluating subexpressions", pos, lo, hi, openParLoc, parenFound)
			if toks[pos].tokType == LEFT_PARAN_TOK {
				openParLoc = pos
			} else if toks[pos].tokType == RIGHT_PARAN_TOK {
				if openParLoc == -1 {
					panic("Mismatched parantheses")
				}
				parenFound = true

				cTree := generateCondTree(toks, openParLoc+1, pos-1, recursionLevel+1)

				t := toks[:openParLoc]
				t = append(t, sqlTokens{CONDITION_TREE_PTR_TOK, cTree})
				if pos+1 <= hi {
					log.Println(recursionLevel, "After the cond tree, appending", pos, toks[pos+1:])
					t = append(t, toks[pos+1:]...)
				}
				toks = t
				hi -= (pos - openParLoc)
				log.Println(recursionLevel, "After the recursive call:", toks, len(toks), pos, lo, hi)
				break
			}
		}

		if parenFound == false {
			if openParLoc != -1 {
				panic("Mismatched parantheses")
			}
			break
		}
	}

	log.Println(recursionLevel, "After parentheses removal, the new bounds are:", lo, hi)

	// Handle AND tokens
	// At this stage, toks[lo:hi] can have only Conditions,
	// ConditionTrees and LogicalOperators.
	//
	// toks[lo:hi] should not have any parantheses
	for pos := lo; pos < hi; pos++ {
		if toks[pos].tokType == AND_TOK {

			cTree := &ConditionTree{}
			cTree.op = AND

			if pos == lo {
				panic("Unexpected AND token")
			}

			// LHS of the AND keyword
			if toks[pos-1].tokType == CONDITION_PTR_TOK {
				cTree.conditions = append(cTree.conditions, *toks[pos-1].value.(*Condition))
			} else if toks[pos-1].tokType == CONDITION_TREE_PTR_TOK {
				cTree.children = append(cTree.children, toks[pos-1].value.(*ConditionTree))
			} else {
				panic("Malformed Query: Unexepected token before AND keyword")
			}

			if (pos + 1) > hi {
				panic("Malformed Query: No token found after AND keyword")
			}

			// RHS of the AND keyword
			if toks[pos+1].tokType == CONDITION_PTR_TOK {
				cTree.conditions = append(cTree.conditions, *toks[pos+1].value.(*Condition))
			} else if toks[pos+1].tokType == CONDITION_TREE_PTR_TOK {
				cTree.children = append(cTree.children, toks[pos+1].value.(*ConditionTree))
			} else {
				panic("Malformed Query: Unexepected token after AND keyword")
			}

			// Insert the new cTree in place of the AND keyword
			// and the tokens on either side
			t := toks[:pos-1]
			t = append(t, sqlTokens{CONDITION_TREE_PTR_TOK, cTree})
			if pos+2 <= hi {
				t = append(t, toks[pos+2:]...)
			}
			toks = t

			// Two tokens are removed in the toks
			hi -= 2

			// This is needed to retain pos in the same position,
			// as the for loop 3rd statement will also increment pos
			pos--
		}
	}
	log.Println(recursionLevel, "After AND evaluation", toks)

	// Handle OR tokens
	// At this stage, toks[lo:hi] can have only Conditions,
	// ConditionTrees and LogicalOperators.
	//
	// toks[lo:hi] should not have any parantheses
	for pos := lo; pos < hi; pos++ {
		if toks[pos].tokType == OR_TOK {

			cTree := &ConditionTree{}
			cTree.op = OR

			if pos == lo {
				panic("Unexpected OR token")
			}

			// LHS of the AND keyword
			if toks[pos-1].tokType == CONDITION_PTR_TOK {
				cTree.conditions = append(cTree.conditions, *toks[pos-1].value.(*Condition))
			} else if toks[pos-1].tokType == CONDITION_TREE_PTR_TOK {
				cTree.children = append(cTree.children, toks[pos-1].value.(*ConditionTree))
			} else {
				panic("Malformed Query: Unexepected token before OR keyword")
			}

			if (pos + 1) > hi {
				panic("Malformed Query: No token found after OR keyword")
			}

			// RHS of the AND keyword
			if toks[pos+1].tokType == CONDITION_PTR_TOK {
				cTree.conditions = append(cTree.conditions, *toks[pos+1].value.(*Condition))
			} else if toks[pos+1].tokType == CONDITION_TREE_PTR_TOK {
				cTree.children = append(cTree.children, toks[pos+1].value.(*ConditionTree))
			} else {
				panic("Malformed Query: Unexepected token after OR keyword")
			}

			// Insert the new cTree in place of the AND keyword
			// and the tokens on either side
			t := toks[:pos-1]
			t = append(t, sqlTokens{CONDITION_TREE_PTR_TOK, cTree})
			if pos+2 <= hi {
				t = append(t, toks[pos+2:]...)
			}
			log.Println(recursionLevel, "toks changed for OR", t)
			toks = t

			// Two tokens are removed in the toks
			hi -= 2

			// This is needed to retain pos in the same position,
			// as the for loop 3rd statement will also increment pos
			pos--
		}
	}

	log.Println(recursionLevel, "After OR evaluation", toks)

	return toks[lo].value.(*ConditionTree)
}
