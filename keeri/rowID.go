// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
//
// Copyright Sankar சங்கர் <sankar.curiosity@gmail.com>

package keeri

import "sort"

// uniquely identifies a row
type rowID uint

// Not threadsafe. Caller should have acquired readlock on arr if needed
func sortAndDeDup(arr []rowID) []rowID {

	m := make(map[int]bool)

	for _, v := range arr {
		m[int(v)] = true
	}

	r := make([]int, 0, len(m))
	for k := range m {
		r = append(r, k)
	}
	sort.Ints(r)

	ret := make([]rowID, len(r))
	for i := 0; i < len(r); i++ {
		ret[i] = rowID(r[i])
	}

	return ret
}
