// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import (
	"bufio"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

func isDelim(r rune) bool {
	return unicode.IsSpace(r) || (r == ',') || (r == '<') || (r == '>') ||
		(r == '=') || (r == '"') || (r == '\'') || (r == '(') || (r == ')')
}

func scanSQLWords(data []byte, atEOF bool) (advance int,
	word []byte, err error) {

	start := 0

	// Scan the word by parsing until a whitespace or a delimiter
	for width, i := 0, start; i < len(data); i += width {
		var r rune
		r, width = utf8.DecodeRune(data[i:])
		if isDelim(r) {
			if i == 0 {
				if r == '\'' || r == '"' {
					for j, innerWidth := 1, 0; j < len(data); j += innerWidth {
						var c rune
						c, innerWidth = utf8.DecodeRune(data[j:])
						if c == r {
							return j + innerWidth, data[1 : j+innerWidth-1], nil
						}
					}
					return 0, nil, errors.New("quote not closed")
				} else if r == '>' || r == '<' {
					l, iw := utf8.DecodeRune(data[1:])
					if l == '=' {
						return width + iw, data[start : width+iw], nil
					}
				} else if unicode.IsSpace(r) {
					// Eliminate all styles of whitespace with
					// a simple blank whitespace, to make the
					// skipEmptyWords function in parser simple.
					//
					// Refer to its history to know what to do
					// if we dont do this simplification here.
					return width, []byte(" "), nil
				}
				return width, data[start:width], nil
			}

			// The below (i+width-1) will be valid only until
			// all the delimiters are only 1 character wide
			return i + width - 1, data[start:i], nil
		}
	}

	// Last word
	if atEOF && len(data) > start {
		return len(data), data[start:], nil
	}

	return start, nil, nil
}

// This function splits the sql string into
// words, relational and logical operators
func splitSQL(input string) ([]string, error) {
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Split(scanSQLWords)

	var ret []string
	for scanner.Scan() {
		ret = append(ret, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return ret, nil
}
