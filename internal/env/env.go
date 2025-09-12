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

package env

import "time"

// LookupEnv is a function that retrieves the value of an environment variable.
type LookupEnv func(key string) (string, bool)

// Env holds the environment configuration.
type Env struct {
	DatabaseURL      string        // the database connection URL
	Endpoint         string        // the S3 endpoint
	Guess            bool          // Guess the URL parameters, no validation.
	LookupEnv        LookupEnv     // allows injection of environment variable lookup for testing
	Path             string        // the S3 bucket path
	Testing          bool          // enables testing mode
	URI              string        // the S3 object URI (if not provided,will be constructed from Endpoint and Path)
	Verbose          bool          // enables verbose logging
	Workers          int           // number of concurrent workers
	WorkloadDuration time.Duration // duration to run the workload
}
