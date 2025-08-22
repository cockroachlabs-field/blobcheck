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

package blob

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/env"
)

func TestCombinations(t *testing.T) {
	tests := []struct {
		name     string
		items    []string
		expected [][]string
	}{
		{
			name:  "empty",
			items: []string{},
			// Power set of empty set is { {} }
			expected: [][]string{{}},
		},
		{
			name:  "single",
			items: []string{"a"},
			expected: [][]string{
				{},
				{"a"},
			},
		},
		{
			name:  "three",
			items: []string{"a", "b", "c"},
			expected: [][]string{
				{},
				{"a"}, {"b"}, {"c"},
				{"a", "b"}, {"a", "c"}, {"b", "c"},
				{"a", "b", "c"},
			},
		},
		{
			name: "s3params",
			items: []string{
				SkipChecksum,
				SkipTLSVerify,
				UsePathStyleParam,
			},
			expected: [][]string{
				{},
				{SkipChecksum}, {SkipTLSVerify}, {UsePathStyleParam},
				{SkipChecksum, SkipTLSVerify}, {SkipChecksum, UsePathStyleParam}, {SkipTLSVerify, UsePathStyleParam},
				{SkipChecksum, SkipTLSVerify, UsePathStyleParam},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			got := combinations(tt.items)
			a.ElementsMatch(got, tt.expected)
		})
	}
}

func TestCandidateConfigs(t *testing.T) {

	tests := []struct {
		name   string
		params Params
		dest   string
		want   []Params
	}{
		{
			name: "basic params and dest",
			params: Params{
				AccountParam: "AKIA...",
				SecretParam:  "SECRET...",
				RegionParam:  "us-east-1",
			},
			dest: "bucket/key",
			want: []Params{
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-east-1"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-east-1", SkipChecksum: "true"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-east-1", SkipTLSVerify: "true"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-east-1", UsePathStyleParam: "true"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-east-1", SkipTLSVerify: "true", SkipChecksum: "true"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-east-1", UsePathStyleParam: "true", SkipChecksum: "true"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-east-1", UsePathStyleParam: "true", SkipTLSVerify: "true"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-east-1", UsePathStyleParam: "true", SkipTLSVerify: "true", SkipChecksum: "true"},
			},
		},
		{
			name: "params with endpoint",
			params: Params{
				AccountParam:  "AKIA...",
				SecretParam:   "SECRET...",
				RegionParam:   "us-west-2",
				EndPointParam: "https://s3.example.com",
			},
			dest: "bucket2/key2",
			want: []Params{
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-west-2", EndPointParam: "https://s3.example.com"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-west-2", EndPointParam: "https://s3.example.com", SkipChecksum: "true"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-west-2", EndPointParam: "https://s3.example.com", SkipTLSVerify: "true"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-west-2", EndPointParam: "https://s3.example.com", UsePathStyleParam: "true"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-west-2", EndPointParam: "https://s3.example.com", SkipTLSVerify: "true", SkipChecksum: "true"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-west-2", EndPointParam: "https://s3.example.com", UsePathStyleParam: "true", SkipChecksum: "true"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-west-2", EndPointParam: "https://s3.example.com", UsePathStyleParam: "true", SkipTLSVerify: "true"},
				{AccountParam: "AKIA...", SecretParam: "SECRET...", RegionParam: "us-west-2", EndPointParam: "https://s3.example.com", UsePathStyleParam: "true", SkipTLSVerify: "true", SkipChecksum: "true"},
			},
		},
		{
			name: "only region param",
			params: Params{
				RegionParam: "eu-central-1",
			},
			dest: "bucket3/key3",
			want: []Params{
				{RegionParam: "eu-central-1"},
				{RegionParam: "eu-central-1", SkipChecksum: "true"},
				{RegionParam: "eu-central-1", SkipTLSVerify: "true"},
				{RegionParam: "eu-central-1", UsePathStyleParam: "true"},
				{RegionParam: "eu-central-1", SkipTLSVerify: "true", SkipChecksum: "true"},
				{RegionParam: "eu-central-1", UsePathStyleParam: "true", SkipChecksum: "true"},
				{RegionParam: "eu-central-1", UsePathStyleParam: "true", SkipTLSVerify: "true"},
				{RegionParam: "eu-central-1", UsePathStyleParam: "true", SkipTLSVerify: "true", SkipChecksum: "true"},
			},
		},
		{
			name:   "empty params",
			params: Params{},
			dest:   "bucket4/key4",
			want: []Params{
				{},
				{SkipChecksum: "true"},
				{SkipTLSVerify: "true"},
				{UsePathStyleParam: "true"},
				{SkipTLSVerify: "true", SkipChecksum: "true"},
				{UsePathStyleParam: "true", SkipChecksum: "true"},
				{UsePathStyleParam: "true", SkipTLSVerify: "true"},
				{UsePathStyleParam: "true", SkipTLSVerify: "true", SkipChecksum: "true"},
			},
		},
		{
			name: "toggle",
			params: Params{
				RegionParam:  "eu-central-1",
				SkipChecksum: "true",
			},
			dest: "bucket3/key3",
			want: []Params{
				{RegionParam: "eu-central-1", SkipChecksum: "true"},
				{RegionParam: "eu-central-1", SkipChecksum: "true", SkipTLSVerify: "true"},
				{RegionParam: "eu-central-1", SkipChecksum: "true", UsePathStyleParam: "true"},
				{RegionParam: "eu-central-1", SkipChecksum: "true", UsePathStyleParam: "true", SkipTLSVerify: "true"},

				{RegionParam: "eu-central-1", SkipChecksum: "false"},
				{RegionParam: "eu-central-1", SkipChecksum: "false", UsePathStyleParam: "true"},
				{RegionParam: "eu-central-1", SkipChecksum: "false", SkipTLSVerify: "true"},
				{RegionParam: "eu-central-1", SkipChecksum: "false", UsePathStyleParam: "true", SkipTLSVerify: "true"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &s3Store{
				params: tt.params,
				dest:   tt.dest,
			}
			gotSeq := s.candidateConfigs()
			var got []Params
			gotSeq(func(d Storage) bool {
				if alt, ok := d.(*s3Store); ok {
					got = append(got, alt.params)
				}
				return true
			})
			assert.Equal(t, len(tt.want), len(got), "candidateConfigs() count mismatch")
			assert.ElementsMatch(t, tt.want, got, "candidateConfigs() elements mismatch")
		})
	}
}
func TestS3ParamsObfuscation(t *testing.T) {
	tests := []struct {
		name   string
		params Params
		want   Params
	}{
		{
			name: "obfuscate secret and token",
			params: Params{
				AccountParam: "AKIA...",
				SecretParam:  "SECRET...",
				TokenParam:   "TOKEN...",
				RegionParam:  "us-east-1",
			},
			want: Params{
				AccountParam: "AKIA...",
				SecretParam:  Obfuscated,
				TokenParam:   Obfuscated,
				RegionParam:  "us-east-1",
			},
		},
		{
			name: "no obfuscation needed",
			params: Params{
				AccountParam: "AKIA...",
				RegionParam:  "us-west-2",
			},
			want: Params{
				AccountParam: "AKIA...",
				RegionParam:  "us-west-2",
			},
		},
		{
			name: "only secret param",
			params: Params{
				SecretParam: "SECRET...",
			},
			want: Params{
				SecretParam: Obfuscated,
			},
		},
		{
			name:   "empty params",
			params: Params{},
			want:   Params{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &s3Store{
				params: tt.params,
				dest:   "bucket/key",
			}
			got := s.Params()
			assert.Equal(t, tt.want, got)
		})
	}
}

const (
	endpoint = "http://localhost:29000"
	account  = "cockroach"
	secret   = "cockroach"
	testPath = "test/minio"
)

func TestMinioFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		env      map[string]string
		endpoint string
		want     Params
		wantErr  error
	}{
		{
			name:     "missing required env vars",
			env:      map[string]string{},
			endpoint: endpoint,
			want:     map[string]string{},
			wantErr:  ErrMissingParam,
		},
		{
			name: "missing secret",
			env: map[string]string{
				AccountParam: account,
			},
			endpoint: endpoint,
			want:     Params{},
			wantErr:  ErrMissingParam,
		},
		{
			name: "missing account",
			env: map[string]string{
				SecretParam: secret,
			},
			endpoint: endpoint,
			want:     Params{},
			wantErr:  ErrMissingParam,
		},
		{
			name: "no region param",
			env: map[string]string{
				AccountParam: account,
				SecretParam:  secret,
			},
			endpoint: endpoint,
			want: Params{
				AccountParam:      account,
				SecretParam:       secret,
				RegionParam:       DefaultRegion,
				EndPointParam:     endpoint,
				UsePathStyleParam: "true",
			},
		},
		{
			name: "region param",
			env: map[string]string{
				AccountParam: account,
				SecretParam:  secret,
				RegionParam:  "us-east-1",
			},
			endpoint: endpoint,
			want: Params{
				AccountParam:      account,
				SecretParam:       secret,
				RegionParam:       "us-east-1",
				EndPointParam:     endpoint,
				UsePathStyleParam: "true",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := stopper.WithContext(t.Context())
			lookup := func(key string) (string, bool) {
				res, ok := tt.env[key]
				return res, ok
			}
			env := &env.Env{
				Path:      testPath,
				Endpoint:  tt.endpoint,
				LookupEnv: lookup,
				Testing:   true,
			}

			blobStorage, err := S3FromEnv(ctx, env)
			if tt.wantErr != nil {
				assert.Nil(t, blobStorage)
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			s3 := (blobStorage.(*s3Store))
			assert.Equal(t, tt.want, s3.params)
			assert.Regexp(t, fmt.Sprintf("^%s", testPath), s3.dest)
		})
	}
}
