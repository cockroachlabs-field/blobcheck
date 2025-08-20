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
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/field-eng-powertools/semver"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/store"
)

// MinVersionForStats is the minimum version required for retrieving statistics.
var MinVersionForStats = semver.MustSemver("v25.1.0")

// ExternalConn represents an external connection to an object store.
type ExternalConn struct {
	name  Ident
	store store.Store
}

// Stats represents statistics about the external connection.
type Stats struct {
	Node        int
	Locality    string
	ErrStr      string
	Transferred string
	ReadSpeed   string
	WriteSpeed  string
	Success     bool
	CanDelete   bool
}

// TableBackup represents a backup of a table.
type TableBackup struct {
	Table   KvTable
	Full    bool
	EndTime time.Time
}

// NewExternalConn creates a new external connection.
func NewExternalConn(
	ctx *stopper.Context, conn *pgxpool.Conn, store store.Store,
) (*ExternalConn, error) {
	extConn := &ExternalConn{
		name:  "_blobcheck_backup",
		store: store,
	}
	err := extConn.Drop(ctx, conn)
	if err != nil {
		return nil, err
	}
	return extConn, extConn.create(ctx, conn)
}

const backupsStmt = `SHOW BACKUPS IN 'external://%[1]s'`

// ListTableBackups lists all table backups in the external connection.
func (c *ExternalConn) ListTableBackups(
	ctx *stopper.Context, conn *pgxpool.Conn,
) ([]string, error) {
	res := make([]string, 0)
	stmt := fmt.Sprintf(backupsStmt, c.String())
	slog.Info(stmt)
	rows, err := conn.Query(ctx, stmt)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		res = append(res, name)
	}
	return res, nil
}

const tableBackupStmt = `
	SELECT backup_type, end_time, parent_schema_name, object_name
	FROM [SHOW BACKUP '%[1]s' IN 'external://%[2]s'] 
	WHERE parent_schema_name='%[3]s' AND object_name='%[4]s'
	ORDER BY end_time DESC`

// BackupInfo retrieves backup information for a specific table.
func (c *ExternalConn) BackupInfo(
	ctx *stopper.Context, conn *pgxpool.Conn, loc string, table KvTable,
) ([]TableBackup, error) {
	res := make([]TableBackup, 0)
	stmt := fmt.Sprintf(tableBackupStmt, loc, c.String(), table.Schema.Name, table.Name)
	rows, err := conn.Query(ctx, stmt)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		t := TableBackup{
			Table: table,
		}
		var tableName, schemeName, backupType string
		if err := rows.Scan(&backupType, &t.EndTime, &schemeName, &tableName); err != nil {
			return nil, err
		}
		t.Full = (backupType == "full")
		slog.Info("backup info", "type", backupType, "full",
			t.Full, "table", tableName, "schema", schemeName)
		res = append(res, t)
	}
	return res, nil
}

const createExtConnStmt = `CREATE EXTERNAL CONNECTION '%[1]s' AS '%[2]s'`

func (c *ExternalConn) create(ctx *stopper.Context, conn *pgxpool.Conn) error {
	destURL := c.store.URL()
	stmt := fmt.Sprintf(createExtConnStmt, c.name, destURL)
	slog.Info("trying", slog.String("url", destURL))
	if _, err := conn.Exec(ctx, stmt); err != nil {
		slog.Info("failed", slog.Any("error", err))
		return err
	}
	slog.Info("checking existing backups")
	backups, err := c.ListTableBackups(ctx, conn)
	if err == nil {
		slog.Info("success",
			slog.String("url", destURL),
			slog.Any("existing", backups))
		return nil
	}
	slog.Info("failed", slog.Any("error", err))
	return errors.Newf("external connection failed")
}

const showExtConnStmt = `SELECT connection_name FROM [SHOW EXTERNAL CONNECTIONS] WHERE connection_name = '%[1]s'`
const dropExtConnStmt = `DROP EXTERNAL CONNECTION '%[1]s';`

// Drop removes the external connection.
func (c *ExternalConn) Drop(ctx *stopper.Context, conn *pgxpool.Conn) error {
	var name string
	err := conn.QueryRow(ctx, fmt.Sprintf(showExtConnStmt, c.name)).Scan(&name)
	if err == pgx.ErrNoRows {
		slog.Info("external connection not found", slog.String("name", name))
		return nil
	}
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, fmt.Sprintf(dropExtConnStmt, c.name))
	return err
}

const checkExtConnStmt = `CHECK EXTERNAL CONNECTION 'external://%[1]s';`

// Stats retrieves statistics for the external connection.
func (c *ExternalConn) Stats(ctx *stopper.Context, conn *pgxpool.Conn) ([]*Stats, error) {
	version, err := Version(ctx, conn)
	if err != nil {
		return nil, err
	}
	if !version.MinVersion(MinVersionForStats) {
		slog.Warn("CockroachDB version is less than 25.1.0. Statistics are not available")
		return nil, nil
	}

	res := make([]*Stats, 0)
	rows, err := conn.Query(ctx, fmt.Sprintf(checkExtConnStmt, c.name))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var stats = &Stats{}
		if err := rows.Scan(
			&stats.Node, &stats.Locality, &stats.Success, &stats.ErrStr,
			&stats.Transferred, &stats.ReadSpeed, &stats.WriteSpeed,
			&stats.CanDelete); err != nil {
			return nil, err
		}
		res = append(res, stats)
	}
	return res, nil
}

// String returns the string representation of the external connection.
func (c *ExternalConn) String() string {
	return string(c.name)
}

// SuggestedParams returns the suggested parameters for the external connection.
func (c *ExternalConn) SuggestedParams() map[string]string {
	return c.store.Params()
}
