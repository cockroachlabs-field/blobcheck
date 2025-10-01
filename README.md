# blobcheck

**blobcheck** is a diagnostic tool for validating object storage connectivity and integration with CockroachDB backup/restore workflows. It verifies that the storage provider is correctly configured, runs synthetic workloads, and produces network performance statistics.  

---

## Usage

```bash
blobcheck s3 [flags]
```

### Global Flags

```text
      --db string                    PostgreSQL connection URL (default "postgresql://root@localhost:26257?sslmode=disable")
      --endpoint string              http endpoint
      --guess                        perform a short test to guess suggested parameters:
                                     it only require access to the bucket; 
                                     it does not try to run a full backup/restore cycle 
                                     in the CockroachDB cluster.
  -h, --help                         help for blobcheck
      --path string                  destination path (e.g. bucket/folder)
      --uri string                   S3 URI
  -v, --verbosity count              increase logging verbosity to debug
      --workers int                  number of concurrent workers (default 5)
      --workload-duration duration   duration of the workload (default 5s)
```

### Credentials

Credentials must be provided in one of the locations supported by `config.LoadDefaultConfig`.  
For example, they can be exported before running:

```bash
export AWS_ACCESS_KEY_ID=..
export AWS_SECRET_ACCESS_KEY=..
```

---

## Examples

### Using endpoint and path

```bash
blobcheck s3 --endpoint http://provider:9000 --path mybucket/cluster1_backup
```

### Using full URI

```bash
blobcheck s3 --uri 's3://mybucket/cluster1_backup?AWS_ACCESS_KEY_ID=..&AWS_SECRET_ACCESS_KEY=..&AWS_ENDPOINT=http://provider:9000'
```

### Sample Output

```text
┌────────────────────────────────────────────────┐
│ Suggested Parameters                           │
├───────────────────────┬────────────────────────┤
│ parameter             │ value                  │
├───────────────────────┼────────────────────────┤
│ AWS_ACCESS_KEY_ID     │ AKIA...                │
│ AWS_ENDPOINT          │ https://s3.example.com │
│ AWS_REGION            │ us-west-2              │
│ AWS_SECRET_ACCESS_KEY │ ******                 │
│ AWS_SKIP_CHECKSUM     │ true                   │
└───────────────────────┴────────────────────────┘
┌──────────────────────────────────────────┐
│ Statistics                               │
├──────┬────────────┬─────────────┬────────┤
│ node │ read speed │ write speed │ status │
├──────┼────────────┼─────────────┼────────┤
│    1 │ 103MB/s    │ 51MB/s      │ OK     │
│    2 │ 101MB/s    │ 50MB/s      │ OK     │
|    3 │ 100MB/s    │ 49MB/s      │ OK     │
└──────┴────────────┴─────────────┴────────┘
```

## Troubleshooting

When issues arise, you can use verbosity flags to understand what’s happening under the hood.

### Enable Debug Output

Running with `-v` enables debug logging. This shows all parameter combinations that `blobcheck` tries when connecting to the storage provider.

For example, connecting to a MinIO server with default settings may fail if virtual host–style requests are used (where the bucket name is treated as part of the hostname):

```text
2025/09/29 14:32:54 DEBUG Trying params env="map[AWS_ACCESS_KEY_ID:cockroach AWS_ENDPOINT:http://localhost:29000 AWS_REGION:aws-global AWS_SECRET_ACCESS_KEY:******]"
2025/09/29 14:32:54 DEBUG Failed to list objects error="operation error S3: ListObjectsV2, https response error StatusCode: 0, RequestID: , HostID: , request send failed, Get \"http://test.localhost:29000/?list-type=2\": dial tcp: lookup test.localhost: no such host" env="map
```

In this case, blobcheck will continue trying alternative combinations until it finds one that works. The first successful combination is then used for backup/restore validation.

### Enable AWS SDK Tracing

Adding a second -v flag provides even deeper insight by enabling AWS SDK trace logs. These include full request/response details exchanged with the storage provider.

Using the same failing MinIO example, the output now shows the full request signature, headers, and why the request failed:


```text
2025/09/29 14:33:51 DEBUG Trying params env="map[AWS_ACCESS_KEY_ID:cockroach AWS_ENDPOINT:http://localhost:29000 AWS_REGION:aws-global AWS_SECRET_ACCESS_KEY:******]"
SDK 2025/09/29 14:33:51 DEBUG Request Signature:
---[ CANONICAL STRING  ]-----------------------------
GET
/
list-type=2
accept-encoding:identity
amz-sdk-invocation-id:4fad79e8-9ae8-43ba-b2a1-7036342f9295
amz-sdk-request:attempt=1; max=1
host:test.localhost:29000
x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
x-amz-date:20250929T183351Z

accept-encoding;amz-sdk-invocation-id;amz-sdk-request;host;x-amz-content-sha256;x-amz-date
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
---[ STRING TO SIGN ]--------------------------------
AWS4-HMAC-SHA256
20250929T183351Z
20250929/us-east-1/s3/aws4_request
7bbccaeea2b0816cf48e8b7e3e916fcb6d641044f7d9fd743c42eca6bcfc018b
-----------------------------------------------------
SDK 2025/09/29 14:33:51 DEBUG Request
GET /?list-type=2 HTTP/1.1
Host: test.localhost:29000
User-Agent: aws-sdk-go-v2/1.39.0 ua/2.1 os/macos lang/go#1.24.2 md/GOOS#darwin md/GOARCH#arm64 api/s3#1.88.1 m/g
Accept-Encoding: identity
Amz-Sdk-Invocation-Id: 4fad79e8-9ae8-43ba-b2a1-7036342f9295
Amz-Sdk-Request: attempt=1; max=1
Authorization: AWS4-HMAC-SHA256 Credential=cockroach/20250929/us-east-1/s3/aws4_request, SignedHeaders=accept-encoding;amz-sdk-invocation-id;amz-sdk-request;host;x-amz-content-sha256;x-amz-date, Signature=e6ed31368624571de7b3b0bb01d658ada05001bb33a20217478c5f09aaaeee55
X-Amz-Content-Sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
X-Amz-Date: 20250929T183351Z

SDK 2025/09/29 14:33:51 DEBUG request failed with unretryable error https response error StatusCode: 0, RequestID: , HostID: , request send failed, Get "http://test.localhost:29000/?list-type=2": dial tcp: lookup test.localhost: no such host
2025/09/29 14:33:51 DEBUG Failed to list objects error="operation error S3: ListObjectsV2, https response error StatusCode: 0, RequestID: , HostID: , request send failed, Get \"http://test.localhost:29000/?list-type=2\": dial tcp: lookup test.localhost: no such host" env="map[AWS_ACCESS_KEY_ID:cockroach AWS_ENDPOINT:http://localhost:29000 AWS_REGION:aws-global AWS_SECRET_ACCESS_KEY:******]"
```

---

## High-Level Architecture

### Components

- **Validator (`internal/validate`)**  
  The central orchestrator for validation. Responsible for:  
  - Database and table creation (source and restored)  
  - Running synthetic workloads  
  - Initiating full and incremental backups  
  - Restoring from backups  
  - Comparing original and restored table fingerprints for integrity verification  

- **Database Layer (`internal/db`)**  
  - Manages creation, dropping, and schema definition for test databases/tables  
  - Handles external connections to the object store  

- **Blob Storage Layer (`internal/blob`)**  
  - Abstracts interactions with the S3 provider  
  - Executes backup/restore commands  
  - Performs quick tests directly on the S3 storage (put/get/list)  

- **Workload Generator (`internal/workload`)**  
  - Populates the source table with synthetic data during tests  
  - Simulates table activity between backups to ensure incremental backups are meaningful  

---

## License

This project is licensed under the Apache 2.0 License. See [LICENSE](LICENSE.txt) for details.
