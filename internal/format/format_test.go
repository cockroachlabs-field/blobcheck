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

package format

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cockroachlabs-field/blobcheck/internal/blob"
	"github.com/cockroachlabs-field/blobcheck/internal/db"
	"github.com/cockroachlabs-field/blobcheck/internal/validate"
)

// Set this to true to rewrite the golden-output files.
const rewriteFiles = false

// Ensure that we can't merge this test if rewrite is true.
func TestRewriteShouldBeFalse(t *testing.T) {
	require.False(t, rewriteFiles)
}

func compareAgainstGoldenFile(filename string, got string, rewriteFiles bool) (bool, error) {
	filePath := filepath.Join(".", "testdata", fmt.Sprintf("%s.txt", filename))
	if rewriteFiles {
		err := os.WriteFile(filePath, []byte(got), 0644)
		if err != nil {
			return false, err
		}
	}
	expected, err := os.ReadFile(filePath)
	if err != nil {
		return false, err
	}
	return got == string(expected), nil
}
func TestReport(t *testing.T) {
	tests := []struct {
		name         string
		report       *validate.Report
		goldenOutput string
	}{
		{
			name: "no stats",
			report: &validate.Report{
				SuggestedParams: blob.Params{
					blob.AccountParam:  "BB...",
					blob.SecretParam:   blob.Obfuscated,
					blob.RegionParam:   "us-east-2",
					blob.EndPointParam: "https://s3.example.com",
				},
				Stats: nil,
			},
			goldenOutput: "no_stats",
		},
		{
			name: "one node",
			report: &validate.Report{
				SuggestedParams: blob.Params{
					blob.AccountParam:  "AKIA...",
					blob.SecretParam:   blob.Obfuscated,
					blob.RegionParam:   "us-west-2",
					blob.EndPointParam: "https://s3.example.com",
					blob.SkipChecksum:  "true",
				},
				Stats: []*db.Stats{
					{
						Node:       1,
						ReadSpeed:  "100MB/s",
						WriteSpeed: "50MB/s",
					},
				}},
			goldenOutput: "one_node",
		},
		{
			name: "two nodes",
			report: &validate.Report{
				SuggestedParams: blob.Params{
					blob.AccountParam:  "AKIA...",
					blob.SecretParam:   blob.Obfuscated,
					blob.RegionParam:   "us-west-2",
					blob.EndPointParam: "https://s3.example.com",
					blob.SkipChecksum:  "true",
				},
				Stats: []*db.Stats{
					{
						Node:       1,
						ReadSpeed:  "100MB/s",
						WriteSpeed: "50MB/s",
					},
					{
						Node:       2,
						ReadSpeed:  "200MB/s",
						WriteSpeed: "100MB/s",
					},
				}},
			goldenOutput: "two_nodes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := require.New(t)
			w := &bytes.Buffer{}
			Report(w, tt.report)
			ok, err := compareAgainstGoldenFile(tt.goldenOutput, w.String(), rewriteFiles)
			a.NoError(err)
			a.True(ok)
		})
	}
}
