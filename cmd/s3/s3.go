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

package s3

import (
	"github.com/spf13/cobra"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/blob"
	"github.com/cockroachlabs-field/blobcheck/internal/env"
	"github.com/cockroachlabs-field/blobcheck/internal/format"
	"github.com/cockroachlabs-field/blobcheck/internal/validate"
)

func command(env *env.Env) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "s3",
		Short: "Performs a validation test for a s3 object store",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := stopper.WithContext(cmd.Context())
			store, err := blob.S3FromEnv(ctx, env)
			if err != nil {
				return err
			}
			validator, err := validate.New(ctx, env, store)
			if err != nil {
				return err
			}
			defer validator.Clean(ctx)
			report, err := validator.Validate(ctx)
			if err != nil {
				return err
			}
			format.Report(cmd.OutOrStdout(), report)
			return nil
		},
	}
	return cmd
}

// Add the command.
func Add(env *env.Env, parent *cobra.Command) {
	cmd := command(env)
	parent.AddCommand(cmd)
}
