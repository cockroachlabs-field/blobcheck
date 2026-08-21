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
	smithyhttp "github.com/aws/smithy-go/transport/http"
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
	// AuthParam tells CockroachDB how to authenticate with the storage provider.
	AuthParam = "AUTH"

	// DefaultRegion is the default AWS region.
	DefaultRegion = "aws-global"
	// AuthImplicit tells CockroachDB to use the node's own environment-based
	// credentials (e.g. an IAM instance role) instead of expecting them in the URL.
	AuthImplicit = "implicit"
)

// ValidParams lists the valid parameters for the S3 object storage.
var ValidParams = []string{
	AccountParam, SecretParam, TokenParam, EndPointParam,
	RegionParam, UsePathStyleParam, SkipChecksum, SkipTLSVerify, AuthParam,
}

var (
	// ObfuscatedParams lists the parameters that should be obfuscated.
	ObfuscatedParams = []string{SecretParam, TokenParam}
	// Obfuscated is the value used to obfuscate sensitive parameters.
	Obfuscated = "******"
)

type s3Store struct {
	params  Params
	dest    string
	verbose bool

	// multiCharDelimiterUnsupported is set when the storage provider rejects
	// a List call using a delimiter longer than one character, e.g. AliCloud
	// OSS. CockroachDB currently sends "data/" as the delimiter (see
	// backupbase.ListingDelimDataSlash) when checking for existing backups
	// and locating incremental/deprecated backup paths.
	multiCharDelimiterUnsupported bool
}

// resolveParams builds the S3 store parameters and destination path from the
// environment. It does not attempt to connect to the storage provider.
func resolveParams(env *env.Env) (Params, string, error) {
	var params Params
	var dest string
	if env.URI != "" {
		var err error
		params, dest, err = extractFromURI(env.URI)
		if err != nil {
			return nil, "", err
		}
	} else {
		// AccountParam and SecretParam are optional here. When absent, the
		// AWS SDK's default credential chain (IAM role, shared credentials
		// file, SSO, etc.) is used instead of static credentials.
		params, _ = lookupEnv(env, nil, []string{AccountParam, SecretParam, TokenParam, RegionParam})
		if env.Endpoint != "" {
			params[EndPointParam] = env.Endpoint
		}
		dest = env.Path
	}

	if _, ok := params[RegionParam]; !ok {
		params[RegionParam] = DefaultRegion
	}
	// When no explicit credentials were supplied, tell CockroachDB to use its
	// own node-level credentials rather than expecting them in the URL.
	_, hasAccount := params[AccountParam]
	_, hasSecret := params[SecretParam]
	if !hasAccount && !hasSecret {
		params[AuthParam] = AuthImplicit
	}
	return params, dest, nil
}

// S3FromEnv creates a new S3 store from the environment.
// It will try to connect to the S3 service using the environment variables provided,
// and adding any parameters that are required.
func S3FromEnv(ctx *stopper.Context, env *env.Env) (Storage, error) {
	params, dest, err := resolveParams(env)
	if err != nil {
		return nil, err
	}
	initial := &s3Store{
		dest:    path.Join(dest, uuid.NewString()),
		params:  params,
		verbose: env.Verbose,
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

// multiCharDelimiterWarning is returned by Warnings when the storage
// provider does not accept a multi-character List delimiter.
const multiCharDelimiterWarning = `storage provider rejects List calls with a multi-character delimiter (e.g. AliCloud OSS); ` +
	`CockroachDB currently sends "data/" as the delimiter when checking for existing backups and locating ` +
	`incremental/deprecated backup paths, so backups and restores against this endpoint may fail`

// Warnings implements BlobStorage.
func (s *s3Store) Warnings() []string {
	if s.multiCharDelimiterUnsupported {
		return []string{multiCharDelimiterWarning}
	}
	return nil
}

// addParam adds a parameter to the S3 store.
func (s *s3Store) addParam(key string, value string) error {
	if slices.Contains(ValidParams, key) {
		s.params[key] = value
		return nil
	}
	return errors.Newf("invalid param %q", key)
}

// combinations returns all subsets (the power set) of the given slice
func combinations(items []string) [][]string {
	var result [][]string
	n := len(items)
	// total number of subsets = 2^n
	total := 1 << n
	for mask := 0; mask < total; mask++ {
		subset := make([]string, 0)
		for i := 0; i < n; i++ {
			if mask&(1<<i) != 0 {
				subset = append(subset, items[i])
			}
		}
		result = append(result, subset)
	}
	return result
}

// candidateConfigs provides a set of candidate configurations for the S3 store.
// TODO(silvano): consider making this public.
func (s *s3Store) candidateConfigs() iter.Seq[Storage] {
	return func(yield func(Storage) bool) {
		combos := combinations([]string{
			SkipChecksum,
			SkipTLSVerify,
			UsePathStyleParam,
		})

		for _, combo := range combos {
			alt := &s3Store{
				dest:   s.dest,
				params: maps.Clone(s.params),
			}
			for _, option := range combo {
				if alt.params[option] == "true" {
					alt.addParam(option, "false")
				} else {
					alt.addParam(option, "true")
				}
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

func extractFromURI(uri string) (Params, string, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, "", err
	}
	if parsed.Scheme != "s3" {
		return nil, "", fmt.Errorf("unsupported scheme: %q", parsed.Scheme)
	}
	res := make(Params)
	for k, v := range parsed.Query() {
		res[k] = v[0]
	}
	return res, path.Join(parsed.Host, parsed.Path), nil
}

// lookupEnv retrieves required and optional environment variables from the provided environment.
func lookupEnv(env *env.Env, required []string, optional []string) (Params, bool) {
	res := make(Params)
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
	var lastErr error
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
		retryMaxAttempts := 1
		addLoadOption(config.WithRetryMaxAttempts(retryMaxAttempts))
		addLoadOption(config.WithClientLogMode(clientMode))
		// Static credentials supplied via --uri or flags (e.g. AWS_ACCESS_KEY_ID)
		// live only in s.params, not in the process environment, so they must be
		// wired in explicitly rather than relying on the SDK's default credential
		// chain to discover them. When neither is set, AuthImplicit is in effect
		// and LoadDefaultConfig falls through to its normal chain (env vars,
		// shared config, IAM role, etc).
		if account, secret := s.params[AccountParam], s.params[SecretParam]; account != "" || secret != "" {
			addLoadOption(config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     account,
					SecretAccessKey: secret,
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
			config.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			config.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}
		s3Client := s3.NewFromConfig(config, func(o *s3.Options) {
			if ep := params[EndPointParam]; ep != "" {
				o.BaseEndpoint = aws.String(ep)
			}
			o.Region = params[RegionParam]
			o.UsePathStyle = usePathStyle
		})

		slog.Info("Trying params", slog.Any("env", alt.Params()))

		if _, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		}); err != nil {
			slog.Debug("Failed to list objects", slog.Any("error", err), slog.Any("env", alt.Params()))
			lastErr = err
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
			slog.Debug("Failed to put object", slog.Any("error", err), slog.Any("env", alt.Params()))
			lastErr = err
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
		if params[SkipTLSVerify] == "true" {
			slog.Warn("TLS verification is disabled; use only for testing")
		}
		// The bare ListObjectsV2 call above (with no Delimiter) already succeeded against
		// this bucket and client, so if adding a multi-character delimiter now causes a
		// client error, the delimiter itself is the only thing that changed and can be
		// held responsible, regardless of which S3-compatible provider is on the other end
		// or how it phrases the error.
		if _, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:    aws.String(bucketName),
			Delimiter: aws.String(multiCharDelimiterProbe),
			MaxKeys:   aws.Int32(1),
		}); err != nil {
			if isDelimiterRejected(err) {
				slog.Warn("storage provider does not support multi-character List delimiters", slog.Any("error", err))
				if altStore, ok := alt.(*s3Store); ok {
					altStore.multiCharDelimiterUnsupported = true
				}
			} else {
				slog.Debug("multi-character delimiter probe failed for an unrelated reason", slog.Any("error", err))
			}
		}
		return alt, nil
	}
	return nil, fmt.Errorf("unable to connect to storage provider %q: %w", s.dest, lastErr)
}

// multiCharDelimiterProbe mirrors CockroachDB's backupbase.ListingDelimDataSlash constant,
// which is used as the S3 List delimiter when checking for existing backups and locating
// incremental/deprecated backup paths.
const multiCharDelimiterProbe = "data/"

// isDelimiterRejected reports whether err is an HTTP 400 (Bad Request) response. It is only
// meaningful when called after an equivalent request without a delimiter has already
// succeeded against the same bucket and client (see the caller in try()), which lets a 400
// here be attributed to the delimiter parameter itself rather than to any provider-specific
// error code or message. This keeps detection provider agnostic: it works the same way for
// AliCloud OSS or any other S3-compatible provider that enforces a similar restriction.
func isDelimiterRejected(err error) bool {
	var respErr *smithyhttp.ResponseError
	if !errors.As(err, &respErr) {
		return false
	}
	return respErr.HTTPStatusCode() == http.StatusBadRequest
}
