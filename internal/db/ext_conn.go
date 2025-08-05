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
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/store"
)

// ExternalConn represents an external connection to a backup store.
type ExternalConn struct {
	name      Ident
	store     store.Store
	suggested store.Dest
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
	SELECT backup_type , end_time, parent_schema_name, object_name
	FROM [SHOW BACKUP '%[1]s' IN 'external://%[2]s'] 
	WHERE object_name='%[3]s'`

// BackupInfo retrieves backup information for a specific table.
func (c *ExternalConn) BackupInfo(
	ctx *stopper.Context, conn *pgxpool.Conn, loc string, table KvTable,
) ([]TableBackup, error) {
	res := make([]TableBackup, 0)
	stmt := fmt.Sprintf(tableBackupStmt, loc, c.String(), table.Name)
	slog.Info(stmt)
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
		slog.Info("backup info", "type", backupType, "full", t.Full, "table", tableName, "schema", schemeName)
		res = append(res, t)
	}
	return res, nil
}

const createExtConnStmt = `CREATE EXTERNAL CONNECTION %[1]s AS '%[2]s'`

func (c *ExternalConn) create(ctx *stopper.Context, conn *pgxpool.Conn) error {
	alt, err := c.store.Suggest(ctx, c.store.BucketName())
	if err != nil {
		return err
	}
	stmt := fmt.Sprintf(createExtConnStmt, c.name, alt.URL())
	slog.Info("trying", slog.String("url", alt.URL()))
	if _, err := conn.Exec(ctx, stmt); err != nil {
		slog.Info("failed", slog.Any("error", err))
		return err
	}
	slog.Info("checking existing backups")
	backups, err := c.ListTableBackups(ctx, conn)
	if err == nil {
		c.suggested = alt
		slog.Info("success",
			slog.String("url", alt.URL()),
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

const checkExtConnStmt = `CHECK EXTERNAL CONNECTION %[1]s;`

// Check verifies the external connection.
func (c *ExternalConn) Check(ctx *stopper.Context, conn *pgxpool.Conn) error {
	rows, err := conn.Query(ctx, fmt.Sprintf(checkExtConnStmt, c.name))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var node int
		var locality, errStr, transferred, readSpeed, writeSpeed string
		var success, canDelete bool
		if err := rows.Scan(&node, &locality, &success, &errStr, &transferred, &readSpeed, &writeSpeed, &canDelete); err != nil {
			return err
		}
		slog.Info("checking external connection",
			"node", node,
			"locality", locality,
			"success", success,
			"error", errStr,
			"transferred", transferred,
			"read_speed", readSpeed,
			"write_speed", writeSpeed,
			"can_delete", canDelete)
	}
	return nil
}

// String returns the string representation of the external connection.
func (c *ExternalConn) String() string {
	return string(c.name)
}

// SuggestedParams returns the suggested parameters for the external connection.
func (c *ExternalConn) SuggestedParams() map[string]string {
	return c.suggested.Params()
}
