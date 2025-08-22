// Copyright 2025 Cockroach Labs, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/store"
)

const (
	testURL     = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
	testBucket  = "test"
	externalURL = "s3://test/backup?AWS_ACCESS_KEY_ID=cockroach&AWS_ENDPOINT=http://localhost:29000&AWS_REGION=us-east-1&AWS_SECRET_ACCESS_KEY=cockroach&&AWS_USE_PATH_STYLE=true"
)

// TestEnv encapsulates the resources needed for testing.
type TestEnv struct {
	Database Database
	KvTable  KvTable
	Store    store.Store
	Pool     *pgxpool.Pool
}

// NewTestEnv creates a new test environment.
func NewTestEnv(ctx *stopper.Context, numRows int) (TestEnv, error) {
	pool, err := pgxpool.New(ctx, "postgresql://root@localhost:26257/defaultdb?sslmode=disable")
	if err != nil {
		return TestEnv{}, err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return TestEnv{}, err
	}
	defer conn.Release()
	db := Database{
		Name: "_test",
	}
	if err := db.Create(ctx, conn); err != nil {
		return TestEnv{}, err
	}
	table := KvTable{
		Database: db,
		Schema:   Schema{"public"},
		Name:     "tmp",
	}
	if err := table.Create(ctx, conn); err != nil {
		return TestEnv{}, err
	}

	for idx := range numRows {
		if err := table.Upsert(ctx, conn, fmt.Sprintf("key-%020d", idx), fmt.Sprintf("value-%020d", idx)); err != nil {
			return TestEnv{}, err
		}
	}
	return TestEnv{
		Database: db,
		KvTable:  table,
		Pool:     pool,
	}, nil
}

// Cleanup removes the resources created for testing.
func (e TestEnv) Cleanup(ctx *stopper.Context) error {
	conn, err := e.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	e.KvTable.Drop(ctx, conn)
	e.Database.Drop(ctx, conn)
	return nil
}

type testStore struct {
}

var _ store.Store = &testStore{}

// BucketName implements store.Store.
func (t *testStore) BucketName() string {
	return testBucket
}

// Params implements store.Store.
func (t *testStore) Params() store.Params {
	return store.Params{}
}

// URL implements store.Store.
func (t *testStore) URL() string {
	return externalURL
}
