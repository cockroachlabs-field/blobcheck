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
	if ctx.IsStopping() {
		return nil
	}
	return v.runConcurrentWorkloadAndBackup(ctx, extConn)
}

// runConcurrentWorkloadAndBackup runs multiple workers and a backup concurrently.
func (v *Validator) runConcurrentWorkloadAndBackup(
	ctx *stopper.Context, extConn *db.ExternalConn,
) error {
	var g sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	// run launches fn under the stopper and records any error it returns.
	// If the stopper is already stopping and rejects the goroutine, the
	// WaitGroup counter is released so that Wait does not block forever.
	run := func(fn func(ctx *stopper.Context) error) {
		g.Add(1)
		accepted := ctx.Go(func(ctx *stopper.Context) error {
			defer g.Done()
			err := fn(ctx)
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
			return err
		})
		if !accepted {
			g.Done()
		}
	}

	// Start worker goroutines.
	for w := range v.env.Workers {
		run(func(ctx *stopper.Context) error {
			slog.Info("starting", "worker", w)
			return v.runWorkload(ctx, v.env.WorkloadDuration)
		})
	}

	// Start the full backup.
	run(func(ctx *stopper.Context) error {
		return v.runFullBackup(ctx, extConn)
	})

	g.Wait()
	slog.Info("workers done")
	return errors.Join(errs...)
}

// runWorkload runs a simple kv-style workload for the specified duration.
func (v *Validator) runWorkload(ctx *stopper.Context, duration time.Duration) error {
	w := workload.Workload{
		Prefix: uuid.New().String(),
		Table:  v.sourceTable,
	}
	done := make(chan bool)

	var g sync.WaitGroup
	var runErr error
	g.Add(1)
	accepted := ctx.Go(func(ctx *stopper.Context) error {
		defer g.Done()
		conn, err := v.pool.Acquire(ctx)
		if err != nil {
			runErr = err
			return err
		}
		defer conn.Release()
		runErr = w.Run(ctx, conn, done)
		return runErr
	})
	if !accepted {
		g.Done()
	}

	select {
	case <-time.Tick(duration):
		// signal workload to stop
		close(done)
	case <-ctx.Stopping():
	}
	g.Wait()
	return runErr
}
