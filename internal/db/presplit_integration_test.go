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
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/cockroachdb/field-eng-powertools/stopper"
)

// TestPresplitMultiNode starts a real 3-node cluster using cockroach-go testserver,
// presplits a table, and asserts that SCATTER distributes leases across more than one node.
func TestPresplitMultiNode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-node test in short mode")
	}

	ts, err := testserver.NewTestServer(testserver.ThreeNodeOpt())
	require.NoError(t, err)
	defer ts.Stop()
	// WaitForInit only waits for node 0; wait for all three before proceeding.
	for i := range 3 {
		require.NoError(t, ts.WaitForInitFinishForNode(i))
	}

	a := assert.New(t)
	r := require.New(t)

	ctx := stopper.WithContext(t.Context())
	pool, err := pgxpool.New(ctx, ts.PGURL().String())
	r.NoError(err)
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	r.NoError(err)
	defer conn.Release()

	db := Database{Name: "_presplit_test"}
	r.NoError(db.Create(ctx, conn))
	defer func() { a.NoError(db.Drop(ctx, conn)) }()

	table := KvTable{
		Database: db,
		Schema:   Schema{"public"},
		Name:     "tmp",
	}
	r.NoError(table.Create(ctx, conn))

	const numRanges = 9 // 3 nodes * rangesPerNode
	r.NoError(table.PresplitAndScatter(ctx, conn, numRanges))

	// Poll until leases spread to more than one node, or timeout. We assert >= 2
	// rather than == 3 because older CRDB versions balance leases more slowly and
	// may not reach full distribution within a reasonable CI timeout. Getting > 1
	// is sufficient to confirm that SCATTER moved leases off a single node.
	r.Eventually(func() bool {
		leaseholders, err := table.LeaseholderCount(ctx, conn)
		return err == nil && leaseholders >= 2
	}, 2*time.Minute, 500*time.Millisecond,
		"SCATTER should distribute leases to more than one node")
}
