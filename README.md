# maria-back-me-up
MariaDB backup tool

## Features
List of features currently available:
- Full dump via mydumper or mysqldump (invertal can be configured)
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

## UI
The UI is available via localhost:8081/
It shows a list of available full backups in S3. Any full backup contains 1 or more incremental backups, which can be selected to perform a complete restore!\
The color of an incremental backups shows the state of the backup verification:\
```
# backup verfication not yet executed
- backup verfication failed
! Backup verfication partly succeeded. A restore was successful, however the table checksum failed
+ backup verfication successful. A restore is save to perform!
```

## Full logical backups
Are done either via the mysql_dump **(default)** or the [mydumper tool](https://github.com/maxbube/mydumper).
Mydumper can use multiple threads to dump and restore tables, makes it therefore suitable for databases with a huge number of tables.
```
full_dump_tool=mysqldump/mydumper
```

## Incremental backups via binlogs
This backup tool uses binlogs to make incremental backups.\
Therefore the binlog needs to be enabled in the MariaDB config
```
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
backup_service:
  full_backup_interval_in_hours: # Interval for full MariaDB dumps in hours
  incremental_backup_interval_in_minutes: # Interval for saving incremental backups
  enable_init_restore: # Enables a automatic restore if one of the databases (in MariaDB.databases) are missing.
  enable_restore_on_db_failure: # Enables automatic restore if the db is unhealthy.\
  outh:
    enabled: # enables OAuth to access the API (openID)\
    provider_url: # Url of the openID provider (e.g. Dex)\
    redirect_url: # OAuth redirect url (this is the url of your mariabackup service)\
  maria_db: # MariaDB config\
    user: # user with admin rights (to drop and restart MariaDB)
    version: # MariaDB version e.g.: "10.4.0"
    password: # user password
    host: # host of the MariaDB instance. If running as a sidecar within the MariaDB pod: 127.0.0.1
    port: # MariaDB port number
    data_dir: # data directory of the MariaDB instance
    databases: # list of databases (used for healthy checks and restores)
      - database_name
      - ... 
      verify_tables: # list of tables for the backup verification check. If none are provided the checksum verification is skipped!
      - database_name.table_name
      - ...
storage_services:
  s3:
    - name: # name of the storage
      aws_access_key_id: # s3 access key
      aws_secret_access_key: # s3 secret access key
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
  maria_db:
    - name: # name of the storage
      host: # host of the MariaDB instance
      port: # MariaDB port number
      user: # MariaDB user with admin rights
      password: # user password
      full_dump_tool: # dump tool used to restore the full dump
      databases: # if specified, only the listed databases are replicated
  disk:
    - base_path: # root folder for the backups
      retention: # backup retention in number of full backups
```
