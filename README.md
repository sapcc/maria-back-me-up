<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company

SPDX-License-Identifier: Apache-2.0
-->

# maria-back-me-up

MariaDB backup tool

## Testing

### Unit Tests

Run unit tests (no external dependencies required):

```bash
go test ./...
```

### Integration Tests

Integration tests require running MariaDB instances. Use the provided script:

```bash
./testing/with-mariadb.sh go test -v -tags integration ./pkg/test/...
```

The script starts MariaDB containers on ports 3306 (primary) and 3307 (streaming target).

## Features

List of features currently available:

- Full dump via MyDumper or mysqldump (interval can be configured)
- Incremental backups via binlog (interval can be configured)
- Supported backup storage
  - S3
  - Swift
  - Disk
- Immutable backups via S3 Object Lock (COMPLIANCE mode: no one, including root, can delete objects before retention expires)
- Client-side encryption of S3 backups (Tink streaming AEAD) with KEK rotation
- Automatic verification of existing backups (can be run as separate service)
- UI to see and select an available backup to restore to
- UI shows status of backup verification
- UI/API can be secured via OAuth openID
- Replication of full dump and binlog events to another MariaDB
  - only QueryEvents are supported
  - restore from this replicas MariaDB is not supported
  - verification of this replica is not supported
  
## Architecture
<img width="605" alt="maria-backup" src="https://user-images.githubusercontent.com/566649/215467649-ac049663-c6fa-4e0e-b21f-25b9af6c187c.png">

## UI

The UI is available via localhost:8081/
It shows a list of available full backups in S3. Any full backup contains 1 or more incremental backups, which can be selected to perform a complete restore!\
Full backups that are client-side encrypted carry a 🔒 badge with the name of the KEK they were encrypted with.\
The color of an incremental backups shows the state of the backup verification:\

```text
# backup verification not yet executed
- backup verification failed
! Backup verification partly succeeded. A restore was successful, however the table checksum failed
+ backup verification successful. A restore is safe to perform!
```

## Full logical backups

Are done either via the mysql_dump **(default)** or the [MyDumper tool](https://github.com/maxbube/mydumper).
MyDumper can use multiple threads to dump and restore tables, makes it therefore suitable for databases with a huge number of tables.

```text
full_dump_tool=mysqldump/mydumper
```

## Incremental backups via binlogs
This backup tool uses binlogs to make incremental backups.\
Therefore the binlog needs to be enabled in the MariaDB config

```text
log-bin=bin.log      # Binlog folder and name
binlog_format=MIXED  # Format, described below
expire_logs_days=3   # After x days binlog files get purged. Important! Otherwise volume could be filling up fast
server_id=1          # Unique server id. Used for replication
```

## Binlog

The tool acts like a replication slave and receives all the binlog events. This allows it to check if any real changes have been made to the database.\
If no changes have been detected, no incremental backup will be created and saved to S3.

## Binlog Format

By default MariaDB will use the MIXED format (since 10.2.4). It is a mix of ROW and STATEMENT.\
ROW will capture the actual change made to a table. The binlog files therefore can get very large.\
e.g. an update to a table column of 1000 rows will create 1000 row changes.
With STATEMENT only the update statement will be recorded in the binlog.

## Config

``` yaml
service_name: # Name of your MariaDB (also used as the s3 folder name)
namespace: # k8s namespace name
sidecar: # boolean
backup:
  full_backup_interval_in_hours: # Interval for full MariaDB dumps in hours
  incremental_backup_interval_in_minutes: # Interval for saving incremental backups, one continuous increment if < 0
  purge_binlog_after_minutes: # if > 0 binlog files are kept on the primary db until they are older
  enable_init_restore: # Enables a automatic restore if one of the databases (in MariaDB.databases) are missing.
  enable_restore_on_db_failure: # Enables automatic restore if the db is unhealthy.\
  disable_binlog_purge_on_rotate: # Boolean to disable binlog purging. Purging is enabled by default
  binlog_max_reconnect_attempts: # Number of reconnect attempts by the binlog syncer, default is 10
  oauth:
    enabled: # enables OAuth to access the API (openID)\
    provider_url: # Url of the openID provider (e.g. Dex)\
    redirect_url: # OAuth redirect url (this is the url of your mariabackup service)\
database: # database config
    type: # either 'mariadb' or 'postgres'
    version: # MariaDB version e.g.: "10.4.0"
    full_dump_tool:
    log_name_format: # prefix of the binlog files
    user: # user with admin rights (to drop and restart MariaDB)
    password: # user password
    host: # host of the MariaDB instance. If running as a sidecar within the MariaDB pod: 127.0.0.1
    port: # MariaDB port number
    server_id: # server_uuid/server_id of the binlog syncer connecting to the host
    data_dir: # data directory of the MariaDB instance
    databases: # list of databases (used for health checks and restores)
      - database_name
      - ...
    verify_tables: # list of tables for the backup verification check. If none are provided the checksum verification is skipped!
      - database_name.table_name
      - ...
storages:
  s3:
    - name: # name of the storage
      aws_access_key_id: # s3 access key
      aws_secret_access_key: # s3 secret access key
      aws_endpoint:
      sse_customer_algorithm:
      s3_force_path_style:
      sse_customer_key:
      cse_active_key: # name of the entry in cse_keys to use for new uploads (empty disables CSE)
      cse_keys: # list of KEKs maria-back-me-up can decrypt with; the active one encrypts. Drop an entry only after retention has aged out the last object that used it.
        - name:  # operator-chosen identifier; recorded in object metadata
          file:  # path to file holding the 32-byte KEK, raw or base64
      region: # s3 region
      bucket_name: # bucket name to save the backup to
      object_lock_enabled: # default false. Set S3 Object Lock on every uploaded object. Requires a bucket with Object Lock enabled; if the bucket doesn't support it, uploads continue without lock and a warning is logged
      object_lock_mode: # COMPLIANCE (default) or GOVERNANCE
      object_lock_retention_days: # required when enabled: per-object lock duration in days. Overrides any bucket default retention rule
  swift:
    - name: # name of the storage
      auth_version: # OpenStack auth version
      auth_url: # OpenStack auth url (keystone url)
      user_name: # os user name
      user_domain_name: # os user domain name
      project_name: # os project name
      project_domain_name: # os project domain name
      password: # os password
      region: # region name
      container_name: # name of the container the backups should be store in
      chunk_size: # default 200mb
      slo_size: # default 600mb
  maria_db:
    - name: # name of the storage
      host: # host of the MariaDB instance
      port: # MariaDB port number
      user: # MariaDB user with admin rights
      password: # user password
      full_dump_tool: # dump tool used to restore the full dump
      databases: # if specified, only the listed databases are replicated
      parse_schema: # if true, the schema is parsed from the SQL Statement of a QueryEvent
      dump_filter_buffer_size_mb: # buffer used for reading from dump, default 2mb
  disk:
    - name: # name of the storage
      base_path: # root folder for the backups
      retention: # backup retention in number of full backups
verification:
  interval_in_minutes: # how often are the backups verified
```

## Client-side encryption (CSE) for S3

The S3 backend supports two encryption modes:

- **SSE-C** (`sse_customer_key`) — server-side; the key transits the wire on every PUT/GET.
- **CSE** (`cse_active_key` + `cse_keys`) — client-side via Tink streaming AEAD (`AES256_GCM_HKDF_1MB`). KEKs never leave the host.

All `cse_keys` entries are folded into one Tink keyset: the `cse_active_key` entry encrypts new uploads, and downloads decrypt by trying every key in the set (trial decryption). Each CSE object carries an `x-amz-meta-cse-key` header naming the KEK that encrypted it — kept for diagnostics and the UI badge, not for routing. The metadata marker keeps S3 keys canonical, lets a bucket mix legacy SSE-C and CSE objects, and supports KEK rotation: add a new entry to `cse_keys`, point `cse_active_key` at it, and the old entry stays readable until retention ages out the last object that used it.

Ciphertext is bound to its S3 object key (used as AAD), so manually moving or renaming an object in the bucket makes it undecryptable — the restore fails instead of returning data from the wrong path.

### Migration from SSE-C to CSE

The two modes coexist: keep `sse_customer_key` set while CSE is on so legacy objects stay readable.

1. Generate a 32-byte KEK into your secret store, e.g. `vault kv put secret/maria-backup/cse value="$(openssl rand -base64 32)"`.
2. Mount it at a known path and add to each `storages.s3[]` entry:

   ```yaml
   cse_active_key: 2026-q3
   cse_keys:
     - name: 2026-q3
       file: /etc/maria-backup/kek-2026-q3
   sse_customer_key: ... # keep during migration
   ```

3. Roll the change. New objects land at canonical S3 keys with an `x-amz-meta-cse-key: 2026-q3` header; legacy SSE-C objects remain readable. Each download starts with a `HeadObject` to choose between the CSE GET and the legacy SSE-C path.
4. Once retention has aged out the last non-CSE object, drop `sse_customer_key`.

### Rotating KEKs

Bring up the new KEK alongside the old one, then promote it:

```yaml
cse_active_key: 2026-q4
cse_keys:
  - name: 2026-q4
    file: /etc/maria-backup/kek-2026-q4
  - name: 2026-q3   # keep until retention ages out the last 2026-q3 object
    file: /etc/maria-backup/kek-2026-q3
```

Old objects continue to decrypt under their original KEK; new uploads use `2026-q4`. Drop the `2026-q3` entry once no live backup still references it.

With several replicas or services sharing a bucket, roll the rotation out in two phases: first add the new entry to `cse_keys` everywhere, then flip `cse_active_key` — that way every reader can already decrypt whatever any writer produces mid-rollout.

Dropping an entry is fail-closed: a backup encrypted under a removed key fails to restore with `no matching key found`, and both the error and the UI badge name the recorded key.

KEKs are the operational secret of record. Losing the active KEK makes new backups unrecoverable; losing an older KEK makes any backup still encrypted under it unrecoverable.

### Logging and the UI

- On startup the S3 backend logs the keyset composition and the active key, e.g. `cse: keyset loaded with 2 key(s) [2026-q4 2026-q3], active (encryption) key "2026-q4"` — the line to check after a rotation rollout.
- Every CSE upload and download logs at debug level the object and the key name involved, .
- The UI backup list badges encrypted full backups with the KEK name from object metadata. A badge naming a key that is no longer in `cse_keys` means that backup cannot currently be restored.

No CSE-specific Prometheus metrics are exposed yet.
