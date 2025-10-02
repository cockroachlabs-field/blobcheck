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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/blob"
	"github.com/cockroachlabs-field/blobcheck/internal/db"
	"github.com/cockroachlabs-field/blobcheck/internal/env"
)

const (
	maxConns                  = 10
	expectedBackupCount       = 2
	expectedBackupCollections = 1
	expectedFullBackupCount   = 1
)

// Report contains the results of a validation run.
type Report struct {
	SuggestedParams blob.Params
	Stats           []*db.Stats
}

// Validator verifies backup/restore functionality
type Validator struct {
	env                        *env.Env
	pool                       *pgxpool.Pool
	blobStorage                blob.Storage
	sourceTable, restoredTable db.KvTable
	latest                     string
}

// New creates a new Validator.
func New(ctx *stopper.Context, env *env.Env, blobStorage blob.Storage) (*Validator, error) {
	if err := preflight(ctx, env, blobStorage); err != nil {
		return nil, err
	}

	config, err := pgxpool.ParseConfig(env.DatabaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse database URL")
	}
	config.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create database pool")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to acquire database connection")
	}
	defer conn.Release()

	sourceTable, err := createSourceTable(ctx, conn)
	if err != nil {
		return nil, err
	}

	// Check for pending jobs on the source table
	pendingJobs, err := sourceTable.PendingJobs(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to check for pending jobs on source table")
	}
	if len(pendingJobs) > 0 {
		slog.Error("pending jobs found on source table. Please review and cancel them.", slog.Any("job_ids", pendingJobs))
		return nil, errors.New("pending jobs found on source table")
	}

	restoredTable, err := createRestoredTable(ctx, conn)
	if err != nil {
		return nil, err
	}

	return &Validator{
		env:           env,
		pool:          pool,
		restoredTable: restoredTable,
		sourceTable:   sourceTable,
		blobStorage:   blobStorage,
	}, nil
}

// preflight validates the input parameters for New.
func preflight(ctx *stopper.Context, env *env.Env, blobStorage blob.Storage) error {
	if env == nil {
		return errors.New("environment cannot be nil")
	}
	if blobStorage == nil {
		return errors.New("blob storage cannot be nil")
	}
	if env.DatabaseURL == "" {
		return errors.New("database URL cannot be empty")
	}
	if env.Workers < 0 {
		return errors.New("workers count cannot be negative")
	}
	if env.WorkloadDuration <= 0 {
		return errors.New("workload duration must be positive")
	}
	return nil
}

// Clean removes all resources created by the validator.
func (v *Validator) Clean(ctx *stopper.Context) error {
	slog.Debug("Starting cleanup of validator resources")
	conn, err := v.acquireConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	var e1, e2 error
	slog.Debug("Dropping source database", slog.String("database", v.sourceTable.Database.String()))
	if err := v.sourceTable.Database.Drop(ctx, conn); err != nil {
		e1 = errors.Wrap(err, "failed to drop source database")
	}
	slog.Debug("Dropping restored database", slog.String("database", v.restoredTable.Database.String()))
	if err := v.restoredTable.Database.Drop(ctx, conn); err != nil {
		e2 = errors.Wrap(err, "failed to drop restored database")
	}
	return errors.Join(e1, e2)
}

// validationStepFn is a function that performs a validation step.
type validationStepFn func(ctx *stopper.Context, extConn *db.ExternalConn) error

// validationStep represents a step in the validation process.
type validationStep struct {
	name string
	fn   validationStepFn
}

// Validate performs a backup/restore against a storage provider
// to asses minimum compatibility at the functional level.
// This does not imply that a storage provider passing the test is supported.
func (v *Validator) Validate(ctx *stopper.Context) (*Report, error) {
	// TODO (silvano): add a progress writer "github.com/jedib0t/go-pretty/v6/progress"
	conn, err := v.acquireConn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	extConn, err := db.NewExternalConn(ctx, conn, v.blobStorage)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create external connection")
	}
	defer extConn.Drop(ctx, conn)

	var stats []*db.Stats

	// Define validation steps
	steps := []validationStep{
		{
			name: "capture initial stats",
			fn: func(ctx *stopper.Context, extConn *db.ExternalConn) error {
				var err error
				stats, err = v.captureInitialStats(ctx, extConn)
				return err
			},
		},
		{
			name: "workload with backup",
			fn:   v.runWorkloadWithBackup,
		},
		{
			name: "incremental backup",
			fn:   v.runIncrementalBackup,
		},
		{
			name: "check backups",
			fn:   v.checkBackups,
		},
		{
			name: "restore",
			fn:   v.performRestore,
		},
		{
			name: "verify integrity",
			fn: func(ctx *stopper.Context, extConn *db.ExternalConn) error {
				if err := v.verifyIntegrity(ctx); err != nil {
					// If we fail to verify the integrity, just log the error, but
					// still provide a complete report
					slog.Error("failed to verify integrity", slog.Any("error", err))
				}
				return nil
			},
		},
	}

	// Execute steps
	for _, step := range steps {
		if ctx.IsStopping() {
			return nil, ctx.Err()
		}
		if err := step.fn(ctx, extConn); err != nil {
			return nil, errors.Wrapf(err, "failed during step: %s", step.name)
		}
	}

	return &Report{
		SuggestedParams: extConn.SuggestedParams(),
		Stats:           stats,
	}, nil
}
