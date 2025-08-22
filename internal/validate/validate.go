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

// Package validate provides functionality to validate backups and restores.
package validate

import (
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/blob"
	"github.com/cockroachlabs-field/blobcheck/internal/db"
	"github.com/cockroachlabs-field/blobcheck/internal/env"
	"github.com/cockroachlabs-field/blobcheck/internal/workload"
)

const (
	maxConns    = 10
	workers     = 5
	defaultTime = 5 * time.Second
)

// Report contains the results of a validation run.
type Report struct {
	SuggestedParams blob.Params
	Stats           []*db.Stats
}

// Validator verifies backup/restore functionality
type Validator struct {
	pool                       *pgxpool.Pool
	blobStorage                blob.Storage
	sourceTable, restoredTable db.KvTable
	latest                     string
}

// New creates a new Validator.
func New(ctx *stopper.Context, env *env.Env, blobStorage blob.Storage) (*Validator, error) {
	config, err := pgxpool.ParseConfig(env.DatabaseURL)
	if err != nil {
		return nil, err
	}
	config.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	source := db.Database{Name: "_blobcheck"}
	if err := source.Create(ctx, conn); err != nil {
		return nil, err
	}
	// TODO (silvano): presplit table to have ranges in all nodes
	sourceTable := db.KvTable{
		Database: source,
		Schema:   db.Public,
		Name:     "mytable",
	}
	if err := sourceTable.Create(ctx, conn); err != nil {
		return nil, err
	}
	dest := db.Database{Name: "_blobcheck_restored"}
	if err := dest.Create(ctx, conn); err != nil {
		return nil, err
	}
	restoredTable := db.KvTable{
		Database: dest,
		Schema:   db.Public,
		Name:     "mytable",
	}
	return &Validator{
		pool:          pool,
		restoredTable: restoredTable,
		sourceTable:   sourceTable,
		blobStorage:   blobStorage,
	}, nil
}

// checkBackups verifies that there is exactly one full and one incremental backup.
func (v *Validator) checkBackups(ctx *stopper.Context, extConn *db.ExternalConn) error {
	conn, err := v.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	backups, err := extConn.ListTableBackups(ctx, conn)
	if err != nil {
		return err
	}
	if len(backups) != 1 {
		return errors.Newf("expected exactly 1 backup collection, got %d", len(backups))
	}
	v.latest = backups[0]
	info, err := extConn.BackupInfo(ctx, conn, backups[0], v.sourceTable)
	if err != nil {
		return err
	}
	if len(info) != 2 {
		return errors.Newf("expected exactly 2 backups (1 full, 1 incremental), got %d backups", len(info))
	}
	fullCount := 0
	for _, i := range info {
		if i.Full {
			fullCount++
		}
	}
	if fullCount != 1 {
		return errors.Newf("expected exactly 1 full backup, got %d", fullCount)
	}
	return nil

}

// Clean removes all resources created by the validator.
func (v *Validator) Clean(ctx *stopper.Context) error {
	conn, err := v.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var e1, e2 error
	if err := v.sourceTable.Database.Drop(ctx, conn); err != nil {
		slog.Error("drop source DB", "err", err)
		e1 = err
	}
	if err := v.restoredTable.Database.Drop(ctx, conn); err != nil {
		slog.Error("drop restored DB", "err", err)
		e2 = err
	}
	return errors.Join(e1, e2)
}

// Validate performs a backup/restore against a storage provider
// to asses minimum compatibility at the functional level.
// This does not imply that a storage provider passing the test is supported.
func (v *Validator) Validate(ctx *stopper.Context) (*Report, error) {
	// TODO (silvano): add a progress writer "github.com/jedib0t/go-pretty/v6/progress"
	conn, err := v.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	extConn, err := db.NewExternalConn(ctx, conn, v.blobStorage)
	if err != nil {
		return nil, err
	}
	defer extConn.Drop(ctx, conn)
	slog.Info("capturing initial statistics")
	// Capture initial stats
	stats, err := extConn.Stats(ctx, conn)
	if err != nil {
		return nil, err
	}

	// write some data
	slog.Info("running workload to populate some data")
	if err := v.runWorkload(ctx, defaultTime); err != nil {
		return nil, err
	}

	g := sync.WaitGroup{}
	for w := range workers {
		g.Add(1)
		ctx.Go(func(ctx *stopper.Context) error {
			defer g.Done()
			slog.Info("starting", "worker", w)
			return v.runWorkload(ctx, defaultTime)
		})
	}
	g.Add(1)
	ctx.Go(func(ctx *stopper.Context) error {
		defer g.Done()
		conn, err := v.pool.Acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()
		slog.Info("starting full backup")
		return v.sourceTable.Backup(ctx, conn, extConn, false)
	})
	g.Wait()
	// Run an incremental backup
	slog.Info("workers done")
	slog.Info("starting incremental backup")
	if err := v.sourceTable.Backup(ctx, conn, extConn, true); err != nil {
		return nil, err
	}

	// Verify that we have the backups, then restore in a separate database.
	if err := v.checkBackups(ctx, extConn); err != nil {
		return nil, err
	}

	slog.Info("restoring backup")
	if err := v.restoredTable.Restore(ctx, conn, extConn, &v.sourceTable); err != nil {
		return nil, err
	}
	slog.Info("checking integrity")
	originalBank, err := v.sourceTable.Fingerprint(ctx, conn)
	if err != nil {
		return nil, err
	}
	restore, err := v.restoredTable.Fingerprint(ctx, conn)
	if err != nil {
		return nil, err
	}

	if originalBank != restore {
		return nil, errors.Errorf("got %s, expected %s while comparing restoreDB with originalBank",
			restore, originalBank)
	}

	return &Report{
		SuggestedParams: extConn.SuggestedParams(),
		Stats:           stats,
	}, nil
}

// runWorkload runs the bank workload for the specified duration.
func (v *Validator) runWorkload(ctx *stopper.Context, duration time.Duration) error {
	// TODO (silvano): if table is presplit, use prefix according to the split
	w := workload.Workload{
		Prefix: uuid.New().String(),
		Table:  v.sourceTable,
	}
	done := make(chan bool)
	ctx.Go(func(ctx *stopper.Context) error {
		conn, err := v.pool.Acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()
		return w.Run(ctx, conn, done)
	})
	select {
	case <-time.Tick(duration):
		// signal workload to stop
		close(done)
	case <-ctx.Stopping():
	}
	return nil
}
