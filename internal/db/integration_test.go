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

package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/field-eng-powertools/stopper"
)

// TestIntegration runs a tests to insure we can create database, table and external connection.
// It tests the end-to-end functionality of the database layer.
func TestIntegration(t *testing.T) {
	numRows := 1000
	a := assert.New(t)
	r := require.New(t)
	ctx := stopper.WithContext(t.Context())
	testEnv, err := NewTestEnv(ctx, numRows)
	r.NoError(err)
	defer func() { a.NoError(testEnv.Cleanup(ctx)) }()
	conn, err := testEnv.Pool.Acquire(ctx)
	r.NoError(err)
	defer conn.Release()
	version, err := Version(ctx, conn)
	r.NoError(err)
	rows, err := conn.Query(ctx, "SELECT v from _test.public.tmp ORDER BY v")
	r.NoError(err)
	idx := 0
	for rows.Next() {
		var v string
		err := rows.Scan(&v)
		r.NoError(err)
		r.Less(idx, numRows)
		a.Equal(fmt.Sprintf("value-%020d", idx), v)
		idx++
	}
	a.Equal(numRows, idx)
	fingerPrint, err := testEnv.KvTable.Fingerprint(ctx, conn)
	r.NoError(err)

	extConn := &ExternalConn{
		name:  "test-conn",
		store: &testStore{},
	}
	extConn.create(ctx, conn)
	defer func() { a.NoError(extConn.Drop(ctx, conn)) }()
	if version.MinVersion(MinVersionForStats) {
		stats, err := extConn.Stats(ctx, conn)
		r.NoError(err)
		a.Equal(len(stats), 1)
	}

	r.NoError(testEnv.KvTable.Backup(ctx, conn, extConn, false))
	targetDB := Database{
		Name: "_test_restore",
	}
	r.NoError(targetDB.Create(ctx, conn))
	defer func() { a.NoError(targetDB.Drop(ctx, conn)) }()
	targetTable := &KvTable{
		Database: targetDB,
		Schema:   Schema{"public"},
		Name:     testEnv.KvTable.Name,
	}

	defer func() { a.NoError(targetTable.Drop(ctx, conn)) }()
	r.NoError(targetTable.Restore(ctx, conn, extConn, &testEnv.KvTable))
	targetFingerprint, err := targetTable.Fingerprint(ctx, conn)
	r.NoError(err)
	a.Equal(fingerPrint, targetFingerprint)
}
