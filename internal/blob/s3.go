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
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachlabs-field/blobcheck/internal/env"
)

const (
	// AccountParam is the AWS access key ID.
	AccountParam = "AWS_ACCESS_KEY_ID"
	// SecretParam is the AWS secret access key.
	SecretParam = "AWS_SECRET_ACCESS_KEY"
	// TokenParam is the AWS session token.
	TokenParam = "AWS_SESSION_TOKEN"
	// EndPointParam is the AWS endpoint.
	EndPointParam = "AWS_ENDPOINT"
	// RegionParam is the AWS region.
	RegionParam = "AWS_REGION"
	// UsePathStyleParam is the AWS use path style.
	UsePathStyleParam = "AWS_USE_PATH_STYLE"
	// SkipChecksum is the AWS skip checksum.
	SkipChecksum = "AWS_SKIP_CHECKSUM"
	// SkipTLSVerify is the AWS skip TLS verify.
	SkipTLSVerify = "AWS_SKIP_TLS_VERIFY"

	// DefaultRegion is the default AWS region.
	DefaultRegion = "aws-global"
)

// ValidParams lists the valid parameters for the S3 object storage.
var ValidParams = []string{
	AccountParam, SecretParam, TokenParam, EndPointParam,
	RegionParam, UsePathStyleParam, SkipChecksum, SkipTLSVerify,
}

var (
	// ObfuscatedParams lists the parameters that should be obfuscated.
	ObfuscatedParams = []string{SecretParam, TokenParam}
	// Obfuscated is the value used to obfuscate sensitive parameters.
	Obfuscated = "******"
)

// ErrMissingParam is returned when required parameters are missing.
var ErrMissingParam = errors.New("AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY must be set")

type s3Store struct {
	params  Params
	dest    string
	testing bool
	verbose bool
}

// S3FromEnv creates a new S3 store from the environment.
// It will try to connect to the S3 service using the environment variables provided,
// and adding any parameters that are required.
func S3FromEnv(ctx *stopper.Context, env *env.Env) (Storage, error) {
	creds, ok := lookupEnv(env, []string{AccountParam, SecretParam}, []string{TokenParam, RegionParam})
	if !ok {
		return nil, ErrMissingParam
	}
	if env.Endpoint != "" {
		creds[EndPointParam] = env.Endpoint
	}
	if _, ok := creds[RegionParam]; !ok {
		creds[RegionParam] = DefaultRegion
	}
	initial := &s3Store{
		dest:    path.Join(env.Path, uuid.NewString()),
		params:  creds,
		testing: env.Testing,
	}
	return initial.try(ctx, initial.BucketName())
}

// BucketName implements BlobStorage.
func (s *s3Store) BucketName() string {
	cleanedPath := path.Clean(s.dest)
	components := strings.Split(cleanedPath, "/")
	if len(components) == 0 {
		return ""
	}
	return components[0]
}

// Params implements BlobStorage.
func (s *s3Store) Params() Params {
	params := maps.Clone(s.params)
	for param := range params {
		if slices.Contains(ObfuscatedParams, param) {
			params[param] = Obfuscated
		}
	}
	return params
}

// URL implements BlobStorage.
func (s *s3Store) URL() string {
	res := s.escapeValues()
	res = fmt.Sprintf("s3://%s?%s", s.dest, res)
	return res
}

// addParam adds a parameter to the S3 store.
func (s *s3Store) addParam(key string, value string) error {
	if slices.Contains(ValidParams, key) {
		s.params[key] = value
		return nil
	}
	return errors.Newf("invalid param %q", key)
}

// candidateConfigs provides a set of candidate configurations for the S3 store.
// TODO(silvano): consider making this public.
func (s *s3Store) candidateConfigs() iter.Seq[Storage] {
	return func(yield func(Storage) bool) {
		combos := [][]string{
			{}, // baseline first
			{SkipChecksum},
			{SkipTLSVerify},
			{UsePathStyleParam},
			{UsePathStyleParam, SkipChecksum},
			{UsePathStyleParam, SkipTLSVerify},
			{UsePathStyleParam, SkipTLSVerify, SkipChecksum},
		}
		for _, combo := range combos {
			alt := &s3Store{
				dest:   s.dest,
				params: maps.Clone(s.params),
			}
			for _, option := range combo {
				alt.addParam(option, "true")
			}
			if !yield(alt) {
				return
			}
		}
	}
}

// escapeValues provides a URL-encoded query string representation of the S3 store parameters.
func (s *s3Store) escapeValues() string {
	var sb strings.Builder
	first := true
	for key, value := range s.params.Iter() {
		if first {
			first = false
		} else {
			sb.WriteString("&")
		}
		sb.WriteString(fmt.Sprintf("%s=%s", url.QueryEscape(key), url.QueryEscape(value)))
	}
	return sb.String()
}

// lookupEnv retrieves required and optional environment variables from the provided environment.
func lookupEnv(env *env.Env, required []string, optional []string) (map[string]string, bool) {
	res := make(map[string]string)
	for _, v := range required {
		val, ok := env.LookupEnv(v)
		if !ok {
			return nil, false
		}
		res[v] = val
	}
	// Add optional environment variables.
	for _, v := range optional {
		val, ok := env.LookupEnv(v)
		if ok {
			res[v] = val
		}
	}
	return res, true
}

const (
	objectKey = "_blobcheck"
	content   = "dummy_data"
)

// try attempts to connect to the S3 store using alternative configurations.
func (s *s3Store) try(ctx context.Context, bucketName string) (Storage, error) {
	var clientMode aws.ClientLogMode
	if s.verbose {
		clientMode |= aws.LogRetries | aws.LogRequestWithBody | aws.LogRequestEventMessage | aws.LogResponse | aws.LogResponseEventMessage | aws.LogSigning
	}
	for alt := range s.candidateConfigs() {
		params := alt.Params()
		var loadOptions []func(options *config.LoadOptions) error
		addLoadOption := func(option config.LoadOptionsFunc) {
			loadOptions = append(loadOptions, option)
		}
		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: params[SkipTLSVerify] == "true"},
			},
		}
		addLoadOption(config.WithHTTPClient(client))
		if params[SkipTLSVerify] == "true" {
			slog.Warn("TLS verification is disabled; use only for testing")
		}
		retryMaxAttempts := 1
		addLoadOption(config.WithRetryMaxAttempts(retryMaxAttempts))
		addLoadOption(config.WithClientLogMode(clientMode))
		// TODO (silvano) - consider removing testing guard
		// LoadDefaultConfig will always honor env based provided credentials if present.
		if s.testing {
			addLoadOption(config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     s.params[AccountParam],
					SecretAccessKey: s.params[SecretParam],
					SessionToken:    s.params[TokenParam],
				}, nil
			})))
		}
		config, err := config.LoadDefaultConfig(ctx, loadOptions...)
		if err != nil {
			return nil, err
		}

		usePathStyle := params[UsePathStyleParam] == "true"
		skipChecksum := params[SkipChecksum] == "true"
		if skipChecksum {
			config.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenSupported
			config.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenSupported
		}
		s3Client := s3.NewFromConfig(config, func(o *s3.Options) {
			if ep := params[EndPointParam]; ep != "" {
				o.BaseEndpoint = aws.String(ep)
			}
			o.Region = params[RegionParam]
			o.UsePathStyle = usePathStyle
		})

		slog.Debug("Trying params", slog.Any("env", alt.Params()))

		if _, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		}); err != nil {
			slog.Debug("Failed to list objects", slog.Any("error", err), slog.Any("env", alt.Params()))
			continue
		}
		// Build a probe key that includes the dest prefix (if any)
		prefix := strings.TrimPrefix(s.dest, s.BucketName())
		prefix = strings.TrimPrefix(prefix, "/")
		probeKey := objectKey
		if prefix != "" {
			probeKey = path.Join(prefix, objectKey)
		}
		// Try to write the object
		input := &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(probeKey),
			Body:   strings.NewReader(content), // Use a reader for the content
		}
		if _, err := s3Client.PutObject(ctx, input); err != nil {
			slog.Error("Failed to put object", slog.Any("error", err), slog.Any("env", alt.Params()))
			continue
		}
		result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(probeKey),
		})
		if err != nil {
			// this shouldn't happen, since we just wrote the object
			return nil, err
		}
		defer result.Body.Close()
		got, err := io.ReadAll(result.Body)
		if err != nil {
			return nil, err
		}
		slog.Debug("Successfully read object", slog.String("content", string(got)))
		if string(got) != content {
			return nil, fmt.Errorf("unexpected content: got %q, want %q", got, content)
		}
		_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(probeKey),
		})
		if err != nil {
			return nil, err
		}
		slog.Debug("Suggested params", slog.Any("env", alt.Params()))
		return alt, nil
	}
	return nil, fmt.Errorf("unable to connect to storage provider %q", s.dest)
}
