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
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/db"
	"github.com/cockroachlabs-field/blobcheck/internal/workload"
)

// runWorkloadWithBackup runs the workload concurrently with a full backup.
func (v *Validator) runWorkloadWithBackup(ctx *stopper.Context, extConn *db.ExternalConn) error {
	slog.Info("running workload to populate some data")
	if err := v.runWorkload(ctx, v.env.WorkloadDuration); err != nil {
		return errors.Wrap(err, "failed to run initial workload")
	}
	return v.runConcurrentWorkloadAndBackup(ctx, extConn)
}

// runConcurrentWorkloadAndBackup runs multiple workers and a backup concurrently.
func (v *Validator) runConcurrentWorkloadAndBackup(
	ctx *stopper.Context, extConn *db.ExternalConn,
) error {
	g := sync.WaitGroup{}
	// Start worker goroutines
	for w := range v.env.Workers {
		g.Add(1)
		ctx.Go(func(ctx *stopper.Context) error {
			defer g.Done()
			return v.runWorkloadWorker(ctx, w)
		})
	}

	// Start backup goroutine
	g.Add(1)
	ctx.Go(func(ctx *stopper.Context) error {
		defer g.Done()
		return v.runFullBackup(ctx, extConn)
	})

	g.Wait()
	slog.Info("workers done")
	return nil
}

// runWorkload runs a simple kv-style workload for the specified duration.
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

// runWorkloadWorker runs a single worker instance.
func (v *Validator) runWorkloadWorker(ctx *stopper.Context, workerID int) error {
	slog.Info("starting", "worker", workerID)
	if err := v.runWorkload(ctx, v.env.WorkloadDuration); err != nil {
		slog.Error("worker failed", "worker", workerID, "error", err)
		return errors.Wrapf(err, "worker %d failed", workerID)
	}
	return nil
}
