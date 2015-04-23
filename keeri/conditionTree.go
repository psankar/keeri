// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import (
	"fmt"
	"strings"
	"sync"
)

type ConditionTree struct {
	op         LogicalOperator
	conditions []*Condition

	children []*ConditionTree
}

// Evaluate the conditions recursively and return the rowIDs
// that match all the conditions recursively. Not threadsafe.
//
// Locks should be handled by the caller, as any panic in this
// recursion should not cause any dangling, stale-locked locks.
// Not threadsafe. Caller should have acquired readlock
func (t *ConditionTree) evaluate() []rowID {

	var wg sync.WaitGroup

	chiRowIDs := make([]([]rowID), len(t.children))
	for i := 0; i < len(t.children); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l := t.children[i].evaluate()
			chiRowIDs[i] = append(chiRowIDs[i], l...)
		}(i)
	}

	conRowIDs := make([]([]rowID), len(t.conditions))
	for i, c := range t.conditions {
		wg.Add(1)
		go func(i int, c *Condition) {
			defer wg.Done()
			l := evaluateCondition(c)
			conRowIDs[i] = append(conRowIDs[i], l...)
		}(i, c)
	}

	wg.Wait()

	var ret []rowID
	if t.op == OR {
		var rows []rowID
		for _, v := range chiRowIDs {
			rows = append(rows, v...)
		}

		for _, v := range conRowIDs {
			rows = append(rows, v...)
		}
		ret = sortAndDeDup(rows)
	} else if t.op == AND {

		// Find the rowIDs that exist in all the
		// individual sets of chiRowIDs and conRowIDs
		var unifiedRowIDs []([]rowID)
		for _, v := range chiRowIDs {
			unifiedRowIDs = append(unifiedRowIDs, v)
		}
		for _, v := range conRowIDs {
			unifiedRowIDs = append(unifiedRowIDs, v)
		}

		foundMap := make(map[rowID]bool)
		notFoundMap := make(map[rowID]bool)

		for i, curArr := range unifiedRowIDs {
			for _, el := range curArr {

				if foundMap[el] == true {
					continue
				}

				if notFoundMap[el] == true {
					continue
				}

				foundCount := 0

			skipEl:
				for j, cmpArr := range unifiedRowIDs {
					if j != i {
						for _, k := range cmpArr {
							// TODO: We need to implement a rowIDCmp function
							// that works similar to strcmp but on the rowID
							// datatype. The == and > below will for now,
							// as long as the rowID is of type int
							if k == el {
								foundCount++
								break
							} else if k > el {
								// el should be added to the notFoundMap
								//
								// 'break label' is used instead of goto,
								// as the code to add to map below is inside
								// an else block and so goto won't work.
								break skipEl
							}
						}
					}
				}

				if foundCount == len(unifiedRowIDs)-1 {
					foundMap[el] = true
				} else {
					// Will come here from the "break skipEl" above
					// And also when we have a number that is bigger than
					// all the other elements in all the other arrays
					notFoundMap[el] = true
				}
			}
		}

		rows := make([]rowID, 0, len(foundMap))
		for k, _ := range foundMap {
			rows = append(rows, k)
		}
		ret = sortAndDeDup(rows)
	} else {
		panic("Not reachable")
	}

	return ret
}

// Not threadsafe. Caller should have acquired readlock
func evaluateCondition(i *Condition) []rowID {
	var ret []rowID

	switch i.colDesc.ColType {
	case IntColumn:
		switch i.op {
		case EQ:
			for k, v := range i.colData.(map[rowID]int) {
				if v == i.value.(int) {
					ret = append(ret, k)
				}
			}
		case NEQ:
			for k, v := range i.colData.(map[rowID]int) {
				if v != i.value.(int) {
					ret = append(ret, k)
				}
			}
		case LT:
			for k, v := range i.colData.(map[rowID]int) {
				if v < i.value.(int) {
					ret = append(ret, k)
				}
			}
		case LTE:
			for k, v := range i.colData.(map[rowID]int) {
				if v <= i.value.(int) {
					ret = append(ret, k)
				}
			}
		case GT:
			for k, v := range i.colData.(map[rowID]int) {
				if v > i.value.(int) {
					ret = append(ret, k)
				}
			}
		case GTE:
			for k, v := range i.colData.(map[rowID]int) {
				if v >= i.value.(int) {
					ret = append(ret, k)
				}
			}
		default:
			panic("Unsupported relational operation for int")
		}
	case StringColumn:
		switch i.op {
		case EQ:
			//TODO: Implement wildcard support
			for k, v := range i.colData.(map[rowID]string) {
				if v == i.value.(string) {
					ret = append(ret, k)
				}
			}
		case NEQ:
			for k, v := range i.colData.(map[rowID]string) {
				if v != i.value.(string) {
					ret = append(ret, k)
				}
			}
		default:
			panic("Unsupported relational operation for string")
		}
	case CustomColumn:
	default:
		panic(fmt.Errorf("Unsupported column type for column %s:%d",
			i.colDesc.ColName, i.colDesc.ColType))
	}

	return ret
}

// TODO: Should evaluate if using the
// `json: tag will help remove some code
// below and thus making json.(Un)Marshal
// to be an alternative to the next function
func (c ConditionTree) String() string {
	chi := ""
	if c.children == nil {
		chi += "\"\""
	} else {
		chi += fmt.Sprintf("[")
		for _, i := range c.children {
			chi += fmt.Sprintf("%s,", i)
		}
		chi = strings.TrimSuffix(chi, ",")
		chi += fmt.Sprintf("]")
	}

	con := ""
	if c.conditions == nil {
		con += "\"\""
	} else {
		con += fmt.Sprintf("[")
		for _, i := range c.conditions {
			con += fmt.Sprintf("%s,", i)
		}
		con = strings.TrimSuffix(con, ",")
		con += fmt.Sprintf("]")
	}

	return fmt.Sprintf("\n{\n\"LogicalOperator\":\"%s\",\n\"Conditions\":%s,\n\"Children\":%s\n}",
		c.op, con, chi)
}
