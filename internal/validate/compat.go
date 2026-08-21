// Copyright 2026 Cockroach Labs, Inc.
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

	"github.com/cockroachdb/field-eng-powertools/semver"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/db"
)

// FixVersionListingDelim is the CockroachDB version starting from which backups no longer
// send a multi-character List delimiter, making warnings about storage providers that reject
// multi-character delimiters (e.g. AliCloud OSS) moot for clusters at or above this version.
// TODO(you): set this once CockroachDB ships a fix (see plans/alicloud.md) so that clusters at
// or above the fixed version stop being flagged.
var FixVersionListingDelim *semver.CockroachVersion // nil: no released fix yet

// checkKnownCompatibilityIssues checks the storage provider for known compatibility issues
// and records those that still apply to the CockroachDB version under test.
func (v *Validator) checkKnownCompatibilityIssues(
	ctx *stopper.Context, extConn *db.ExternalConn,
) error {
	warnings := v.blobStorage.Warnings()
	if len(warnings) == 0 {
		return nil
	}
	conn, err := v.acquireConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	version, err := db.Version(ctx, conn)
	if err != nil {
		return err
	}
	if !listingDelimWarningApplies(version) {
		slog.Debug("known compatibility issues do not apply to this CockroachDB version",
			slog.String("version", version.String()))
		return nil
	}
	for _, w := range warnings {
		slog.Warn(w)
	}
	v.warnings = append(v.warnings, warnings...)
	return nil
}

// listingDelimWarningApplies reports whether the multi-character-delimiter warning still
// applies to the given CockroachDB version.
func listingDelimWarningApplies(version *semver.CockroachVersion) bool {
	return FixVersionListingDelim == nil || !version.MinVersion(FixVersionListingDelim)
}
