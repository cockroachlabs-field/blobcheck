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

package backup

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/cmd/env"
	"github.com/cockroachlabs-field/blobcheck/internal/db"
	"github.com/cockroachlabs-field/blobcheck/internal/store"
	"github.com/cockroachlabs-field/blobcheck/internal/workload"
)

var defaultTime = 5 * time.Second

// Validator verifies backup/restore functionality
type Validator struct {
	pool                       *pgxpool.Pool
	store                      store.Store
	sourceTable, restoredTable db.KvTable
	latest                     string
}

// New creates a new Validator.
func New(ctx *stopper.Context, env *env.Env, store store.Store) (*Validator, error) {
	pool, err := pgxpool.New(ctx, env.DatabaseURL)
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
		store:         store,
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
		return errors.Newf("Invalid number of backups %d", len(backups))
	}
	v.latest = backups[0]
	info, err := extConn.BackupInfo(ctx, conn, backups[0], v.sourceTable)
	if err != nil {
		return err
	}
	if len(info) != 2 {
		return errors.Newf("Invalid number of backups %d", len(info))
	}
	for _, i := range info {
		slog.Info("res", "full", i.Full)
	}
	if (info[0].Full && !info[1].Full) || (info[1].Full && !info[0].Full) {
		return nil
	}
	return errors.Errorf("expecting full and incremental")

}

// Clean removes all resources created by the validator.
func (v *Validator) Clean(ctx *stopper.Context) error {
	conn, err := v.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	var errs error
	if err := v.sourceTable.Database.Drop(ctx, conn); err != nil {
		errs = err
		fmt.Println(err)
	}
	if err := v.restoredTable.Database.Drop(ctx, conn); err != nil {
		fmt.Println(err)
	}
	return errors.Join(errs, err)
}

// Validate performs a backup/restore against a storage provider
// to asses minimum compatibility at the functional level.
// This does not imply that a storage provider passing the test is supported.
func (v *Validator) Validate(ctx *stopper.Context) error {

	conn, err := v.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	extConn, err := db.NewExternalConn(ctx, conn, v.store)
	if err != nil {
		return err
	}
	defer extConn.Drop(ctx, conn)
	// write some data
	slog.Info("running workload to populate some data")
	if err := v.runWorload(ctx, defaultTime); err != nil {
		return err
	}

	childCtx := stopper.WithContext(ctx)
	childCtx.Go(func(ctx *stopper.Context) error {
		conn, err := v.pool.Acquire(ctx)
		if err != nil {
			return err
		}
		slog.Info("starting full backup")
		return v.sourceTable.Backup(ctx, conn, extConn, false)
	})
	childCtx.Go(func(ctx *stopper.Context) error {
		err := v.runWorload(ctx, defaultTime)
		ctx.Stop(time.Second)
		return err
	})
	childCtx.Wait()

	// Run an incremental backup
	slog.Info("starting incremental backup")
	if err := v.sourceTable.Backup(ctx, conn, extConn, true); err != nil {
		return err
	}

	// Verify that we have the backups, then restore in a separate database.
	if err := v.checkBackups(ctx, extConn); err != nil {
		return err
	}

	slog.Info("restoring backup")
	if err := v.restoredTable.Restore(ctx, conn, extConn, &v.sourceTable); err != nil {
		return err
	}

	originalBank, err := v.sourceTable.Fingerprint(ctx, conn)
	if err != nil {
		return err
	}
	restore, err := v.restoredTable.Fingerprint(ctx, conn)
	if err != nil {
		return err
	}

	if originalBank != restore {
		return (errors.Errorf("got %s, expected %s while comparing restoreDB with originalBank",
			restore, originalBank))
	}
	err = extConn.Check(ctx, conn)
	if err != nil {
		return err
	}
	slog.Info("Suggested", slog.Any("Params", extConn.SuggestedParams()))

	return nil
}

// runWorload runs the bank workload for the specified duration.
func (v *Validator) runWorload(ctx *stopper.Context, duration time.Duration) error {

	w := workload.Workload{
		Table: v.sourceTable,
	}
	done := make(chan bool)
	defer close(done)
	ctx.Go(func(ctx *stopper.Context) error {
		conn, err := v.pool.Acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()
		return w.Run(ctx, conn, done)
	})
	ticker := time.NewTicker(duration)
	select {
	case <-ticker.C:
		done <- true
	case <-ctx.Stopping():
	}
	return nil
}
