# blobcheck

**blobcheck** is a diagnostic tool for validating object storage connectivity and integration with CockroachDB backup/restore workflows. It verifies that the storage provider is correctly configured, runs synthetic workloads, and produces network performance statistics.  

---

## Usage

```bash
blobcheck s3 [flags]
```

### Flags

```
  -h, --help   help for s3
```

### Global Flags

```
      --db string         PostgreSQL connection URL 
                          (default "postgresql://root@localhost:26257?sslmode=disable")
      --endpoint string   http endpoint, if uri is not specified
      --path string       destination path (e.g. bucket/folder), if uri is not specified
      --uri string        in the [scheme]://[host]/[path]?[parameters] format
      --verbose           increase logging verbosity to debug
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
│    1 │ 100MB/s    │ 50MB/s      │ OK     │
│    2 │ 200MB/s    │ 100MB/s     │ OK     │
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
