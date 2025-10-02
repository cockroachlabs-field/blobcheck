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

package validate

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/db"
)

// acquireConn acquires a database connection from the pool.
func (v *Validator) acquireConn(ctx *stopper.Context) (*pgxpool.Conn, error) {
	conn, err := v.pool.Acquire(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to acquire database connection")
	}
	return conn, nil
}

// captureInitialStats captures initial database statistics.
func (v *Validator) captureInitialStats(
	ctx *stopper.Context, extConn *db.ExternalConn,
) ([]*db.Stats, error) {
	conn, err := v.acquireConn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	slog.Info("capturing initial statistics")
	stats, err := extConn.Stats(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to capture initial statistics")
	}
	return stats, nil
}

// createSourceTable creates the source database and table.
func createSourceTable(ctx *stopper.Context, conn *pgxpool.Conn) (db.KvTable, error) {
	source := db.Database{Name: "_blobcheck"}
	if err := source.Create(ctx, conn); err != nil {
		return db.KvTable{}, errors.Wrap(err, "failed to create source database")
	}

	// TODO (silvano): presplit table to have ranges in all nodes
	sourceTable := db.KvTable{
		Database: source,
		Schema:   db.Public,
		Name:     "mytable",
	}
	if err := sourceTable.Create(ctx, conn); err != nil {
		return db.KvTable{}, errors.Wrap(err, "failed to create source table")
	}
	return sourceTable, nil
}

// createRestoredTable creates the restored database and table.
func createRestoredTable(ctx *stopper.Context, conn *pgxpool.Conn) (db.KvTable, error) {
	dest := db.Database{Name: "_blobcheck_restored"}
	if err := dest.Create(ctx, conn); err != nil {
		return db.KvTable{}, errors.Wrap(err, "failed to create restored database")
	}

	restoredTable := db.KvTable{
		Database: dest,
		Schema:   db.Public,
		Name:     "mytable",
	}
	return restoredTable, nil
}
