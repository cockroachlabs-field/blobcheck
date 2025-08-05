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

package store

import "context"

// Dest represents the parameters to be set for a destination to perform a backup/restore.
type Dest interface {
	// Params returns a copy of the params.
	Params() map[string]string
	// URL returns a escaped URL.
	URL() string
	// BucketName returns the name of the bucket.
	BucketName() string
}

// Store represents a storage backend.
type Store interface {
	Dest
	// Suggest returns the suggested parameters for a backup/restore.
	Suggest(ctx context.Context, bucketName string) (Dest, error)
}
