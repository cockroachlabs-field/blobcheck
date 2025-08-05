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

package backup

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/cmd/env"
	"github.com/cockroachlabs-field/blobcheck/internal/store"
)

func createMinioBucket(
	ctx *stopper.Context, vars map[string]string, env *env.Env, bucketName string,
) error {
	minioClient, err := minio.New("localhost:9000", &minio.Options{
		Creds: credentials.NewStaticV4(vars["AWS_ACCESS_KEY_ID"], vars["AWS_SECRET_ACCESS_KEY"], ""),
	})
	if err != nil {
		return err
	}
	found, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		return err
	}
	if found {
		slog.Info("Bucket already exists", slog.String("bucket", bucketName))
	}
	minioClient.MakeBucket(ctx, bucketName,
		minio.MakeBucketOptions{
			Region:        "us-east-1",
			ObjectLocking: false,
		},
	)
	return nil

}

func TestValidation(t *testing.T) {
	ctx := stopper.WithContext(t.Context())
	r := require.New(t)
	vars := map[string]string{
		"AWS_ACCESS_KEY_ID":     "cockroach",
		"AWS_SECRET_ACCESS_KEY": "cockroach",
		"AWS_REGION":            "us-east-1",
	}
	lookup := func(key string) (string, bool) {
		val, ok := vars[key]
		return val, ok
	}
	bucketName := fmt.Sprintf("bucket-%d", time.Now().UnixMilli())
	var env = &env.Env{
		DatabaseURL: "postgresql://root@localhost:26257?sslmode=disable",
		Endpoint:    "http://localhost:9000",
		LookupEnv:   lookup,
		Path:        bucketName,
	}
	r.NoError(createMinioBucket(ctx, vars, env, bucketName))
	store, err := store.S3FromEnv(env)
	r.NoError(err)
	validator, err := New(ctx, env, store)
	r.NoError(err)
	defer validator.Clean(ctx)
	r.NoError(validator.Validate(ctx))
}
