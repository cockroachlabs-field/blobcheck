// Copyright 2025 Cockroach Labs, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHexSeedKeys(t *testing.T) {
	a := assert.New(t)

	a.Nil(hexSeedKeys(0), "0 ranges should return nil")

	one := hexSeedKeys(1)
	require.Len(t, one, 1)
	a.Equal("00000000-0000-0000-0000-000000000001", one[0])

	keys := hexSeedKeys(4)
	require.Len(t, keys, 4, "4 ranges should yield 4 seed keys")
	a.Equal("00000000-0000-0000-0000-000000000001", keys[0])
	a.Equal("40000000-0000-0000-0000-000000000001", keys[1])
	a.Equal("80000000-0000-0000-0000-000000000001", keys[2])
	a.Equal("c0000000-0000-0000-0000-000000000001", keys[3])

	// Each key must sort strictly above the previous one, and each key must
	// sort above the corresponding split boundary.
	splitPts := hexSplitPoints(4) // ["4000", "8000", "c000"]
	for i := 1; i < len(keys); i++ {
		a.Greater(keys[i], keys[i-1], "seed keys must be strictly increasing")
		a.GreaterOrEqual(keys[i], splitPts[i-1], "seed key[%d] must be >= split point[%d]", i, i-1)
	}
	// First key must fall below the first split boundary.
	a.Less(keys[0], splitPts[0], "first seed key must be below first split boundary")
}

func TestHexSplitPoints(t *testing.T) {
	a := assert.New(t)

	a.Nil(hexSplitPoints(0), "0 ranges should return nil")
	a.Nil(hexSplitPoints(1), "1 range should return nil")

	pts := hexSplitPoints(4)
	require.Len(t, pts, 3, "4 ranges should yield 3 boundaries")
	a.Equal([]string{"4000", "8000", "c000"}, pts)

	for _, p := range pts {
		a.Regexp(`^[0-9a-f]{4}$`, p, "boundary must be 4 lowercase hex chars")
	}

	large := hexSplitPoints(300)
	require.Len(t, large, 299)
	a.True(sort.StringsAreSorted(large), "boundaries must be sorted")
	seen := make(map[string]bool, len(large))
	for _, p := range large {
		a.False(seen[p], "duplicate boundary: %s", p)
		seen[p] = true
	}
}
