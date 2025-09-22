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
