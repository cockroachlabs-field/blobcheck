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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cockroachdb/field-eng-powertools/semver"
)

func TestListingDelimWarningApplies(t *testing.T) {
	tests := []struct {
		name       string
		fixVersion *semver.CockroachVersion
		version    *semver.CockroachVersion
		want       bool
	}{
		{
			name:       "no released fix always warns",
			fixVersion: nil,
			version:    semver.MustSemver("v25.4.0"),
			want:       true,
		},
		{
			name:       "cluster below the fix version still warns",
			fixVersion: semver.MustSemver("v25.4.0"),
			version:    semver.MustSemver("v25.3.0"),
			want:       true,
		},
		{
			name:       "cluster at the fix version does not warn",
			fixVersion: semver.MustSemver("v25.4.0"),
			version:    semver.MustSemver("v25.4.0"),
			want:       false,
		},
		{
			name:       "cluster above the fix version does not warn",
			fixVersion: semver.MustSemver("v25.4.0"),
			version:    semver.MustSemver("v26.1.0"),
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := FixVersionListingDelim
			defer func() { FixVersionListingDelim = old }()
			FixVersionListingDelim = tt.fixVersion

			assert.Equal(t, tt.want, listingDelimWarningApplies(tt.version))
		})
	}
}
