// Copyright 2025 Cockroach Labs, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParamsIter verifies that Params.Iter yields keys and values in sorted order.
func TestParamsIter(t *testing.T) {
	p := Params{
		"zeta":     "last",
		"alpha":    "first",
		"middle":   "second",
		"beta":     "third",
		"delta":    "fourth",
		"epsilon":  "fifth",
		"gamma":    "sixth",
		"Aardvark": "seventh", // capital letter to test ASCII ordering
	}

	var gotKeys []string
	var gotVals []string
	for k, v := range p.Iter() {
		gotKeys = append(gotKeys, k)
		gotVals = append(gotVals, v)
	}

	// Keys should be sorted in standard Go string order (ASCII/UTF-8 ordering).
	wantKeys := []string{
		"Aardvark",
		"alpha",
		"beta",
		"delta",
		"epsilon",
		"gamma",
		"middle",
		"zeta",
	}
	wantVals := []string{
		"seventh",
		"first",
		"third",
		"fourth",
		"fifth",
		"sixth",
		"second",
		"last",
	}
	a := assert.New(t)
	a.Equal(gotKeys, wantKeys)
	a.Equal(gotVals, wantVals)
}
