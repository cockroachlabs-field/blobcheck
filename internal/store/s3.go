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
	"path/filepath"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/cockroachdb/errors"
	"github.com/cockroachlabs-field/blobcheck/cmd/env"
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

	// NoRegion is the default AWS region.
	NoRegion = "no-region"
)

// ValidParams lists the valid parameters for the S3 store.
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
	params map[string]string
	dest   string
}

// S3FromEnv creates a new S3 store from the environment.
func S3FromEnv(env *env.Env) (Store, error) {
	creds, ok := lookupEnv(env, []string{AccountParam, SecretParam}, []string{TokenParam, RegionParam})
	if !ok {
		return nil, ErrMissingParam
	}
	if env.Endpoint != "" {
		creds[EndPointParam] = env.Endpoint
	}
	if _, ok := creds[RegionParam]; !ok {
		creds[RegionParam] = NoRegion
	}
	return &s3Store{
		dest:   path.Join(env.Path, uuid.NewString()),
		params: creds,
	}, nil
}

// Alternates implements Store.
func (s *s3Store) alternates() iter.Seq[Dest] {
	return func(yield func(Dest) bool) {
		combos := [][]string{
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

// Params implements Store.
func (s *s3Store) Params() map[string]string {
	params := maps.Clone(s.params)
	for param := range params {
		if slices.Contains(ObfuscatedParams, param) {
			params[param] = Obfuscated
		}
	}
	return params
}

// URL implements Store.
func (s *s3Store) URL() string {
	res := s.escapeValues()
	res = fmt.Sprintf("s3://%s?%s", s.dest, res)
	return res
}

// BucketName implements Store.
func (s *s3Store) BucketName() string {
	bucket, _ := filepath.Split(s.dest)
	return strings.TrimSuffix(bucket, "/")
}

func (s *s3Store) addParam(key string, value string) error {
	if slices.Contains(ValidParams, key) {
		s.params[key] = value
		return nil
	}
	return errors.Newf("invalid param %q", key)
}

func (s *s3Store) escapeValues() string {
	var sb strings.Builder
	first := true
	for key, value := range s.params {
		if first {
			first = false
		} else {
			sb.WriteString("&")
		}
		sb.WriteString(fmt.Sprintf("%s=%s", key, url.QueryEscape(value)))
	}
	return sb.String()
}

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
	content   = "<blobcheck>"
)

// Suggest implements Store.
func (s *s3Store) Suggest(ctx context.Context, bucketName string) (Dest, error) {

	var clientMode aws.ClientLogMode
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		clientMode |= aws.LogRetries | aws.LogRequestWithBody | aws.LogRequestEventMessage | aws.LogResponse | aws.LogResponseEventMessage | aws.LogSigning
	}

	for alt := range s.alternates() {
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
		retryMaxAttempts := 1
		addLoadOption(config.WithRetryMaxAttempts(retryMaxAttempts))
		//addLoadOption(config.WithLogger(newLogAdapter(ctx)))
		addLoadOption(config.WithClientLogMode(clientMode))

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
			o.BaseEndpoint = aws.String(params[EndPointParam])
			o.Region = params[RegionParam]
			o.UsePathStyle = usePathStyle
			slog.Info("S3 Client Options", slog.Any("options", o))
		})
		fmt.Printf("++++ UsePathStyle: %v\n", s3Client.Options().UsePathStyle)
		fmt.Printf("++++ Bucket: %v\n", bucketName)

		slog.Info("Trying params", slog.Any("env", alt))

		if _, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		}); err != nil {
			slog.Error("Failed to list objects", slog.Any("error", err), slog.Any("env", alt))
			continue
		}
		// Try to write the object
		input := &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(content), // Use a reader for the content
		}
		if _, err := s3Client.PutObject(ctx, input); err != nil {
			slog.Error("Failed to put object", slog.Any("error", err), slog.Any("env", alt))
			continue
		}
		result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			// we were able to write this object, it must exists.
			return nil, err
		}
		defer result.Body.Close()
		got, err := io.ReadAll(result.Body)
		if err != nil {
			return nil, err
		}
		slog.Info("Successfully read object", slog.String("content", string(got)))
		if string(got) != content {
			return nil, fmt.Errorf("unexpected content: got %q, want %q", got, content)
		}
		_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			return nil, err
		}
		slog.Info("Suggested params", slog.Any("env", alt))
		return alt, nil
	}

	return nil, fmt.Errorf("unable to connect to storage provider %q: ", s.dest)
}
