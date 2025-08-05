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

package workload

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/db"
)

// Workload represents a workload to be run.
type Workload struct {
	// Table is the database table to operate on.
	Table db.KvTable
}

// Run executes the workload.
func (w *Workload) Run(ctx *stopper.Context, conn *pgxpool.Conn, done <-chan bool) error {
	ticker := time.NewTicker(time.Millisecond)
	for {
		err := w.Table.Insert(ctx, conn, uuid.NewString())
		if err != nil {
			return err
		}
		select {
		case <-done:
			return nil
		case <-ctx.Stopping():
			return nil
		case <-ticker.C:
		}
	}
}
