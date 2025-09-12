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

package cmd

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/cockroachlabs-field/blobcheck/cmd/s3"
	"github.com/cockroachlabs-field/blobcheck/internal/env"
)

var verbosity int
var envConfig = &env.Env{
	DatabaseURL: "postgresql://root@localhost:26257?sslmode=disable",
	LookupEnv:   os.LookupEnv,
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "blobcheck",
	Short: "blobcheck validates backup/restore operation against blob storage",
	Long: `blobcheck is a diagnostic tool for validating object storage connectivity 
and integration with CockroachDB backup/restore workflows. 
It verifies that the storage provider is correctly configured, 
runs synthetic workloads, and produces network performance statistics.`,
	PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
		if envConfig.DatabaseURL == "" && !envConfig.Guess {
			return errors.New("database URL cannot be blank")
		}
		if envConfig.URI != "" {
			if envConfig.Endpoint != "" || envConfig.Path != "" {
				return errors.New("URI and (endpoint + path) cannot be set simultaneously")
			}
		} else {
			if envConfig.Endpoint == "" {
				return errors.New("set (endpoint + path) or URI")
			}
			if envConfig.Path == "" {
				return errors.New("set (endpoint + path) or URI")
			}
		}
		if verbosity > 0 {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}
		if verbosity > 1 {
			envConfig.Verbose = true
		}
		return nil
	},
}

// Execute runs the root command.
func Execute() {
	s3.Add(envConfig, rootCmd)
	f := rootCmd.PersistentFlags()
	f.StringVar(&envConfig.DatabaseURL, "db", envConfig.DatabaseURL, "PostgreSQL connection URL")
	f.StringVar(&envConfig.Path, "path", envConfig.Path, "destination path (e.g. bucket/folder)")
	f.StringVar(&envConfig.Endpoint, "endpoint", envConfig.Path, "http endpoint")
	f.StringVar(&envConfig.URI, "uri", envConfig.URI, "S3 URI")
	f.BoolVar(&envConfig.Guess, "guess", false, `perform a short test to guess suggested parameters:
it only require access to the bucket; 
it does not try to run a full backup/restore cycle 
in the CockroachDB cluster.`)
	f.CountVarP(&verbosity, "verbosity", "v", "increase logging verbosity to debug")
	f.IntVar(&envConfig.Workers, "workers", 5, "number of concurrent workers")
	f.DurationVar(&envConfig.WorkloadDuration, "workload-duration", 5*time.Second, "duration of the workload")
	err := rootCmd.Execute()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
