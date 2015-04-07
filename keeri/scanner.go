// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import (
	"bufio"
	"strings"
	"unicode"
	"unicode/utf8"
)

func isDelim(r rune) bool {
	return unicode.IsSpace(r) || (r == ',') || (r == '<') || (r == '>') ||
		(r == '=') || (r == '"') || (r == '\'') || (r == '(') || (r == ')')
}

func scanTokens(data []byte, atEOF bool) (advance int,
	token []byte, err error) {

	start := 0

	// Scan the token by parsing until a whitespace or a delimiter
	for width, i := 0, start; i < len(data); i += width {
		var r rune
		r, width = utf8.DecodeRune(data[i:])
		if isDelim(r) {
			if i == 0 {
				return width, data[start:width], nil
			}

			// The below (i+width-1) will be valid only until
			// all the delimiters are only 1 character wide
			return i + width - 1, data[start:i], nil
		}
	}

	// Last token
	if atEOF && len(data) > start {
		return len(data), data[start:], nil
	}

	return start, nil, nil
}

func tokenize(input string) ([]string, error) {
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Split(scanTokens)

	var ret []string
	for scanner.Scan() {
		ret = append(ret, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return ret, nil
}
