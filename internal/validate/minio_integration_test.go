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

package validate

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/blob"
	"github.com/cockroachlabs-field/blobcheck/internal/db"
	"github.com/cockroachlabs-field/blobcheck/internal/env"
)

const minioEndpoint = "localhost:29000"

func createMinioBucket(
	ctx *stopper.Context, vars map[string]string, env *env.Env, bucketName string,
) error {
	minioClient, err := minio.New(minioEndpoint, &minio.Options{
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
		slog.Debug("Bucket already exists", slog.String("bucket", bucketName))
	}
	minioClient.MakeBucket(ctx, bucketName,
		minio.MakeBucketOptions{
			Region:        "us-east-1",
			ObjectLocking: false,
		},
	)
	return nil

}

// TestMinio performs a minimal validation test against a MinIO instance.
func TestMinio(t *testing.T) {
	ctx := stopper.WithContext(t.Context())
	r := require.New(t)
	endpoint := fmt.Sprintf("http://%s", minioEndpoint)
	vars := blob.Params{
		blob.AccountParam: "cockroach",
		blob.SecretParam:  "cockroach",
		blob.RegionParam:  "us-east-1",
	}
	expected := blob.Params{
		blob.AccountParam:      "cockroach",
		blob.SecretParam:       blob.Obfuscated,
		blob.RegionParam:       "us-east-1",
		blob.EndPointParam:     endpoint,
		blob.UsePathStyleParam: "true",
	}
	lookup := func(key string) (string, bool) {
		val, ok := vars[key]
		return val, ok
	}

	bucketName := fmt.Sprintf("bucket-%d", time.Now().UnixMilli())
	var env = &env.Env{
		DatabaseURL: "postgresql://root@localhost:26257?sslmode=disable",
		Endpoint:    endpoint,
		LookupEnv:   lookup,
		Path:        bucketName,
		Testing:     true,
	}
	r.NoError(createMinioBucket(ctx, vars, env, bucketName))
	blobStorage, err := blob.S3FromEnv(ctx, env)
	r.NoError(err)
	validator, err := New(ctx, env, blobStorage)
	r.NoError(err)
	defer validator.Clean(ctx)
	report, err := validator.Validate(ctx)
	r.NoError(err)
	r.NotNil(report)
	// Validate the report contents
	r.NotEmpty(report.SuggestedParams)
	r.Equal(expected, report.SuggestedParams)
	// Validate the report stats, if applicable
	conn, err := validator.pool.Acquire(ctx)
	r.NoError(err)
	defer conn.Release()
	version, err := db.Version(ctx, conn)
	r.NoError(err)
	if version.MinVersion(db.MinVersionForStats) {
		r.Equal(1, len(report.Stats))
	}
}
