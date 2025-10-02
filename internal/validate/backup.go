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

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/db"
)

// checkBackups verifies that there is exactly one full and one incremental backup.
func (v *Validator) checkBackups(ctx *stopper.Context, extConn *db.ExternalConn) error {
	conn, err := v.acquireConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	backups, err := extConn.ListTableBackups(ctx, conn)
	if err != nil {
		return errors.Wrap(err, "failed to list table backups")
	}
	if len(backups) != expectedBackupCollections {
		return errors.Newf("expected exactly %d backup collection, got %d", expectedBackupCollections, len(backups))
	}

	v.latest = backups[0]
	info, err := extConn.BackupInfo(ctx, conn, backups[0], v.sourceTable)
	if err != nil {
		return errors.Wrap(err, "failed to get backup info")
	}
	if len(info) != expectedBackupCount {
		return errors.Newf("expected exactly %d backups (1 full, 1 incremental), got %d backups", expectedBackupCount, len(info))
	}

	fullCount := 0
	for _, i := range info {
		if i.Full {
			fullCount++
		}
	}
	if fullCount != expectedFullBackupCount {
		return errors.Newf("expected exactly %d full backup, got %d", expectedFullBackupCount, fullCount)
	}
	return nil
}

// performRestore restores the backup to a separate database.
func (v *Validator) performRestore(ctx *stopper.Context, extConn *db.ExternalConn) error {
	conn, err := v.acquireConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	slog.Info("restoring backup")
	if err := v.restoredTable.Restore(ctx, conn, extConn, &v.sourceTable); err != nil {
		return errors.Wrap(err, "failed to restore backup")
	}
	return nil
}

// runFullBackup runs a full backup in a separate database connection.
func (v *Validator) runFullBackup(ctx *stopper.Context, extConn *db.ExternalConn) error {
	conn, err := v.acquireConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	slog.Info("starting full backup")
	if err := v.sourceTable.Backup(ctx, conn, extConn, false); err != nil {
		return errors.Wrap(err, "failed to create full backup")
	}
	return nil
}

// runIncrementalBackup runs an incremental backup.
func (v *Validator) runIncrementalBackup(ctx *stopper.Context, extConn *db.ExternalConn) error {
	conn, err := v.acquireConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	slog.Info("starting incremental backup")
	if err := v.sourceTable.Backup(ctx, conn, extConn, true); err != nil {
		return errors.Wrap(err, "failed to create incremental backup")
	}
	return nil
}

// verifyIntegrity checks that the restored data matches the original.
func (v *Validator) verifyIntegrity(ctx *stopper.Context) error {
	conn, err := v.acquireConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	slog.Info("checking integrity")
	original, err := v.sourceTable.Fingerprint(ctx, conn)
	if err != nil {
		return errors.Wrap(err, "failed to get original table fingerprint")
	}

	restore, err := v.restoredTable.Fingerprint(ctx, conn)
	if err != nil {
		return errors.Wrap(err, "failed to get restored table fingerprint")
	}

	if original != restore {
		return errors.Errorf("integrity check failed: got %s, expected %s while comparing restored data with original",
			restore, original)
	}
	return nil
}
