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
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/field-eng-powertools/stopper"
)

func TestDB(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)
	ctx := stopper.WithContext(context.Background())

	pool, err := pgxpool.New(ctx, "postgresql://root@localhost:26257/defaultdb?sslmode=disable")
	r.NoError(err)
	conn, err := pool.Acquire(ctx)
	r.NoError(err)
	defer conn.Release()
	db := Database{
		Name: "_test",
	}
	err = db.Create(ctx, conn)
	r.NoError(err)
	defer conn.Exec(ctx, "DROP DATABASE _test CASCADE")
	table := KvTable{
		Database: db,
		Schema:   Schema{"public"},
		Name:     "tmp",
	}
	err = table.Create(ctx, conn)
	r.NoError(err)
	values := []string{"a", "b", "c"}
	for _, row := range values {
		err = table.Insert(ctx, conn, row)
		r.NoError(err)
	}
	rows, err := conn.Query(ctx, "SELECT v from _test.public.tmp ORDER BY v")
	r.NoError(err)
	idx := 0
	for rows.Next() {
		var v string
		err := rows.Scan(&v)
		r.NoError(err)
		r.Less(idx, len(values))
		a.Equal(values[idx], v)
		idx++
	}
	a.Equal(len(values), idx)
}
