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
	slog.Info("Dropping table", slog.String("table", t.String()))
	_, err := conn.Exec(ctx, fmt.Sprintf(dropTableStmt, t.String()))
	return err
}

const insertTableStmt = `
INSERT INTO %[1]s (k, v) values (@key, @value);`

// Insert adds a new row to the table.
func (t *KvTable) Insert(ctx *stopper.Context, conn *pgxpool.Conn, key, value string) error {
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
	slog.Info(stmt)
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
