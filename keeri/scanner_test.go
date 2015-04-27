// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import "testing"

func Test_splitSQL(t *testing.T) {

	cases := []struct {
		info  string
		query string
		want  []string
	}{
		{"Simple select query with just a tablename",
			"SELECT * FROM tab1",
			[]string{"SELECT", " ", "*", " ", "FROM", " ", "tab1"}},

		{"Query with a single WHERE clause returning a single column",
			"SELECT col1 from table1 WHERE col1 > 1",
			[]string{"SELECT", " ", "col1", " ", "from", " ", "table1", " ", "WHERE",
				" ", "col1", " ", ">", " ", "1"}},

		{"Query with two conditions and one logical operator returning two columns",
			"SELECT col1 , col2 from table1 WHERE col1 > 1 AND col2 < 1",
			[]string{"SELECT", " ", "col1", " ", ",", " ", "col2", " ", "from",
				" ", "table1", " ", "WHERE", " ", "col1", " ", ">", " ", "1",
				" ", "AND", " ", "col2", " ", "<", " ", "1"}},

		{"Multi line sql query with paranthesised conditions, both AND and OR operators, embedded spaces in parameters",
			`SELECT col1, col2, col3 FROM table1 WHERE
	 (col1 > 18 AND col2='  f ' OR status!='s' AND (col1 <= 2 OR col2 >=1) )`,
			[]string{"SELECT", " ", "col1", ",", " ", "col2", ",", " ", "col3",
				" ", "FROM", " ", "table1", " ", "WHERE", " ", " ", " ", "(",
				"col1", " ", ">", " ", "18", " ", "AND", " ", "col2", "=", "  f ",
				" ", "OR", " ", "status", "!=", "s", " ", "AND", " ", "(",
				"col1", " ", "<=", " ", "2", " ", "OR", " ", "col2", " ", ">=",
				"1", ")", " ", ")"}},
	}

	for _, i := range cases {
		got, err := splitSQL(i.query)
		if err != nil {
			t.Error(err)
		}

		if len(got) != len(i.want) {
			t.Errorf("\nFAIL:%s\nWant: '%v'\nGot: '%v'\n", i.info, i.want, got)
		} else {
			for k, v := range got {
				if v != i.want[k] {
					t.Errorf("\nFAIL:%s\nWant: '%v' Got: '%v'\n", i.info, i.want[k], v)
				}
			}
		}

	}
}
