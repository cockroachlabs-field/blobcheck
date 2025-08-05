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

	"github.com/spf13/cobra"

	"github.com/cockroachlabs-field/blobcheck/cmd/env"
	"github.com/cockroachlabs-field/blobcheck/cmd/s3"
)

var debug bool
var envConfig = &env.Env{
	DatabaseURL: "postgresql://root@localhost:26257/_tempo?sslmode=disable",
	LookupEnv:   os.LookupEnv,
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "blobcheck",
	Short: "blobcheck validates backup/restore operation against blob storage",
	PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
		if envConfig.DatabaseURL == "" {
			return errors.New("database URL cannot be blank")
		}
		if envConfig.Endpoint == "" {
			return errors.New("endpoint cannot be blank")
		}
		if envConfig.Path == "" {
			return errors.New("path cannot be blank")
		}
		if debug {
			slog.SetLogLoggerLevel(slog.LevelDebug)
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
	f.BoolVar(&debug, "verbose", false, "increase logging verbosity to debug")
	err := rootCmd.Execute()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
