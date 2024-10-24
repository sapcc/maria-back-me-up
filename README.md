# maria-back-me-up

MariaDB backup tool

## Features

List of features currently available:

- Full dump via MyDumper or mysqldump (invertal can be configured)
- Incremental backups via binlog (invertal can be configured)
- Supported backup storage
  - S3
  - Swift
  - Disk
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
The color of an incremental backups shows the state of the backup verification:\

```text
# backup verfication not yet executed
- backup verfication failed
! Backup verfication partly succeeded. A restore was successful, however the table checksum failed
+ backup verfication successful. A restore is save to perform!
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
binlog_format=MIXED  # Formant, described below
expire_logs_days=3   # After x days binlog files get purged. Important! Otherwise volume could be filling up fast
server_id=1          # Unique server id. Used for replication
```

## Binlog

The tool acts like a replication slave and receives all the binlog events. This allows it to check if any real changes have been made to the database.\
If no changes have been detected, no incremental backup will be created and saved to S3.

## Binlog Format

By default MariaDB will use the MIXED format (since 10.2.4). It is a mix of ROW and STATEMENT.\
ROW will capture the actual change made to a table. The binlog files therefor can get very large.\
e.g. an update to a table column of 1000 rows will create 1000 row changes.
With STATEMENT only the update statement will be recorded in the binlog.

## Config

``` yaml
service_name: # Name of your MariaDB (also used as the s3 folder name)
namespace: # k8s namespace name
sidecar: # boolean
backup:
  full_backup_interval_in_hours: # Interval for full MariaDB dumps in hours
  incremental_backup_interval_in_minutes: # Interval for saving incremental backups, one continous increment if < 0
  purge_binlog_after_minutes: # if > 0 binlog files are kept on the primary db until they are older
  enable_init_restore: # Enables a automatic restore if one of the databases (in MariaDB.databases) are missing.
  enable_restore_on_db_failure: # Enables automatic restore if the db is unhealthy.\
  disable_binlog_purge_on_rotate: # Boolean to disable binlog purging. Purging is enabled by detault
  binlog_max_reconnect_attempts: # Number of reconnect attempts by the binlog syncer, default is 10
  outh:
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
      region: # s3 region
      bucket_name: # bucket name to save the backup to
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
