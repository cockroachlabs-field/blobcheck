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
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cockroachdb/field-eng-powertools/stopper"
)

// KvTable represents a key-value table in the database.
type KvTable struct {
	Database
	Schema
	Name Ident
}

const backupTableStmt = `BACKUP %[1]s INTO %[2]s 'external://%[3]s'`

// Backup creates a backup of the table.
func (t *KvTable) Backup(
	ctx *stopper.Context, conn *pgxpool.Conn, dest *ExternalConn, incremental bool,
) error {
	mod := ""
	if incremental {
		mod = "LATEST IN"
	}
	_, err := conn.Exec(ctx, fmt.Sprintf(backupTableStmt, t.String(), mod, dest))
	return err
}

const createTableStmt = `
CREATE TABLE IF NOT EXISTS %[1]s (
  k string DEFAULT gen_random_uuid()::STRING PRIMARY KEY,
  v string
);`

// Create creates the table.
func (t *KvTable) Create(ctx *stopper.Context, conn *pgxpool.Conn) error {
	_, err := conn.Exec(ctx, fmt.Sprintf(createTableStmt, t.String()))
	return err
}

const dropTableStmt = `
DROP TABLE IF EXISTS %[1]s;`

// Drop removes the table.
func (t *KvTable) Drop(ctx *stopper.Context, conn *pgxpool.Conn) error {
	slog.Debug("Dropping table", slog.String("table", t.String()))
	_, err := conn.Exec(ctx, fmt.Sprintf(dropTableStmt, t.String()))
	return err
}

const insertTableStmt = `
UPSERT INTO %[1]s (k, v) values (@key, @value);`

// Upsert adds a new row to the table.
func (t *KvTable) Upsert(ctx *stopper.Context, conn *pgxpool.Conn, key, value string) error {
	_, err := conn.Exec(ctx, fmt.Sprintf(insertTableStmt, t.String()), pgx.NamedArgs{
		"key":   key,
		"value": value,
	})
	return err
}

const restoreTableStmt = `RESTORE %[1]s  FROM '%[2]s' IN 'external://%[3]s' WITH into_db=%[4]s`

// Restore restores the table from a backup.
func (t *KvTable) Restore(
	ctx *stopper.Context, conn *pgxpool.Conn, from *ExternalConn, original *KvTable,
) error {
	stmt := fmt.Sprintf(restoreTableStmt, original.String(), "LATEST", from, t.Database.Name)
	slog.Debug(stmt)
	_, err := conn.Exec(ctx, stmt)
	return err
}

// String returns the string representation of the table.
func (t *KvTable) String() string {
	return strings.Join([]string{t.Database.String(), t.Schema.String(), string(t.Name)}, ".")
}

// LocalName returns the local name of the table.
func (t *KvTable) LocalName() string {
	return strings.Join([]string{t.Schema.String(), string(t.Name)}, ".")
}

// hexSplitPoints returns ranges-1 evenly spaced hex-prefix boundaries that
// divide the gen_random_uuid() keyspace into `ranges` roughly equal parts.
// Returns nil when ranges < 2. Boundaries are 4 hex digits wide, giving 65536
// distinct buckets, which is ample for any realistic node count.
func hexSplitPoints(ranges int) []string {
	if ranges < 2 {
		return nil
	}
	const buckets = 0x10000
	points := make([]string, 0, ranges-1)
	for i := 1; i < ranges; i++ {
		points = append(points, fmt.Sprintf("%04x", i*buckets/ranges))
	}
	return points
}

const splitTableStmt = `ALTER TABLE %[1]s SPLIT AT VALUES %[2]s`

// Split presplits the table at the given primary-key boundaries.
func (t *KvTable) Split(ctx *stopper.Context, conn *pgxpool.Conn, points []string) error {
	if len(points) == 0 {
		return nil
	}
	slog.Debug("splitting table", slog.String("table", t.String()), slog.Any("split_points", points))
	quoted := make([]string, len(points))
	for i, p := range points {
		quoted[i] = fmt.Sprintf("('%s')", p)
	}
	stmt := fmt.Sprintf(splitTableStmt, t.String(), strings.Join(quoted, ", "))
	_, err := conn.Exec(ctx, stmt)
	return err
}

const scatterTableStmt = `ALTER TABLE %[1]s SCATTER`

// Scatter distributes the table's ranges across the cluster nodes.
func (t *KvTable) Scatter(ctx *stopper.Context, conn *pgxpool.Conn) error {
	_, err := conn.Exec(ctx, fmt.Sprintf(scatterTableStmt, t.String()))
	return err
}

// PresplitAndScatter presplits the table into `ranges` ranges and scatters
// them across the cluster so that backup is more likely to exercise every node.
func (t *KvTable) PresplitAndScatter(ctx *stopper.Context, conn *pgxpool.Conn, ranges int) error {
	if err := t.Split(ctx, conn, hexSplitPoints(ranges)); err != nil {
		return err
	}
	return t.Scatter(ctx, conn)
}

// hexSeedKeys returns one seed key per range, each positioned just above the
// range's lower boundary. Seeding gives scattered ranges data to export so
// backup is more likely to contact every node. The first range uses prefix
// "0000"; subsequent ranges use the corresponding split boundary from
// hexSplitPoints. Returns nil when ranges < 1.
func hexSeedKeys(ranges int) []string {
	if ranges < 1 {
		return nil
	}
	points := hexSplitPoints(ranges) // len == ranges-1
	prefixes := make([]string, ranges)
	prefixes[0] = "0000"
	for i, p := range points {
		prefixes[i+1] = p
	}
	keys := make([]string, ranges)
	for i, pfx := range prefixes {
		// Construct a UUID-shaped string whose leading 4 hex chars match the
		// range prefix so lexicographic ordering places it in the right range.
		keys[i] = pfx + "0000-0000-0000-0000-000000000001"
	}
	return keys
}

// SeedRanges inserts one deterministic row per range so that scattered ranges
// have data to export during backup. Call after PresplitAndScatter.
func (t *KvTable) SeedRanges(ctx *stopper.Context, conn *pgxpool.Conn, ranges int) error {
	for _, k := range hexSeedKeys(ranges) {
		if err := t.Upsert(ctx, conn, k, "seed"); err != nil {
			return err
		}
	}
	return nil
}

const leaseholderCountStmt = `SELECT count(DISTINCT lease_holder) FROM [SHOW RANGES FROM TABLE %[1]s WITH DETAILS]`

// LeaseholderCount returns the number of distinct nodes currently holding
// leases for the table's ranges. Used to observe how SCATTER distributed the
// ranges before a backup.
func (t *KvTable) LeaseholderCount(ctx *stopper.Context, conn *pgxpool.Conn) (int, error) {
	var n int
	if err := conn.QueryRow(ctx, fmt.Sprintf(leaseholderCountStmt, t.String())).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

const fingerprintStmt = `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s`

// Fingerprint returns a fingerprint for the table.
func (t *KvTable) Fingerprint(ctx *stopper.Context, conn *pgxpool.Conn) (string, error) {
	var b strings.Builder
	query := fmt.Sprintf(fingerprintStmt, t.String())
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var name, fp string
		if err := rows.Scan(&name, &fp); err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "%s: %s\n", name, fp)
	}
	return b.String(), rows.Err()
}

const jobsStmt = `
SELECT job_id
FROM [SHOW JOBS]
WHERE
  NOT status = ANY (@status)
  AND description LIKE @desc
`

var pendingStatues = []string{"succeeded", "failed"}

// PendingJobs returns a list of job IDs that are still pending (not succeeded or failed).
func (t *KvTable) PendingJobs(ctx *stopper.Context, conn *pgxpool.Conn) ([]int64, error) {
	slog.Debug("Checking for pending jobs", slog.String("table", t.String()))
	rows, err := conn.Query(ctx, jobsStmt, pgx.NamedArgs{
		"status": pendingStatues,
		"desc":   fmt.Sprintf("%%%s%%", t.Name),
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowTo[int64])
}
