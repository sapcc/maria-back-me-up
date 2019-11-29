# maria-back-me-up
Mariadb backup tool

## Feature
List of features currently available:
- Full dump via mydumper (invertal can be configured)
- Incremental backups via binlog (invertal can be configured)
- Supported backup storage
    - S3
- Automatic verification of existing backups
- UI to see and select an available backup to restore to
- UI shows status of verfications

## UI
The UI is available via localhost:8081/
It shows a list of available full backups in S3. Any full backup contains 1 or more incremental backups, which can selected to perform a complete restore!\
The color of an incremental backups shows the state of the backup verfication:\
- <span style="color:grey">backup check not yet executed</span>
- <span style="color:red">backup check failed</span>
- <span style="color:orange">Backup check partly succeeded. A restore was successful, however the table checksum failed</span>
- <span style="color:green">backup check successful</span>


## Incremental backups via binlogs
This backup tool uses binlogs to make incremental backups.\ 
Therefore the binlog needs to be enabled in the mariadb config
```
log-bin=bin.log      # Binlog folder and name\
binlog_format=MIXED  # Formant, described below\
expire_logs_days=3   # After x days binlog files get purged. Important! Otherwise volume could be filling up fast\
server_id=1          # Unique server id. Used for replication\
```

### Binlog
The tool acts like a replication slave and recieves all the binlog events. This allows it to check if any real changes have been made to the dabase.\
If no changes have been detected, no incremental backup will be created and saved to S3.


### Binlog Format
By default Mariadb will use the MIXED format (since 10.2.4). It is a mix of ROW and STATEMENT.\
ROW will capture the actual change made to a table. The binlog files therefor can get very large.\
e.g. an update to a table column of 1000 rows will create 1000 row changes. 
With STATEMENT only the update statement will be recorded in the binlog.


## Config
`full_backup_interval_in_hours:` Interval for full mariadb dumps in hours\
`incremental_backup_interval_in_minutes:` Interval for saving incremental backups\
`service_name:` Name of your mariadb (also used as the s3 folder name)\
`namespace:` k8s namespace name\
`enable_init_restore:` Enables init restore if one of the databases (in mariadb.databases) are missing.\
`maria_db:` mariadb config\
  `user:` user with admin rights (to drop and restart mariabd)\
  `version:` mariadb version e.g.: "10.4.0"\
  `password:` user password\
  `host:` host of the mariabd instance. If running as a sidecar within the mariadb pod: 127.0.0.1\
  `port:` mariadb port number\
  `data_dir:` data directory of the mariadb instance\
  `databases:` list of databases (used for healthy checks and restores)\
    - database_name\
    - ...\
  `verify_tables:` list of tables for the backup verification check. If none are provided the checksum verficiation is skipped!\
    - database_name.table_name\
    - ...\
`s3:`\
  `aws_access_key_id:` s3 acces key\
  `aws_secret_access_key:` s3 secret access key\
  `region:` s3 region\
  `bucket_name:` bucket name to save the backup to\