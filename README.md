# maria-back-me-up
Mariadb backup tool

## Features
List of features currently available:
- Full dump via mydumper or mysqldump (invertal can be configured)
- Incremental backups via binlog (invertal can be configured)
- Supported backup storage
    - S3
    - Swift
- Automatic verification of existing backups (can be run as separte service)
- UI to see and select an available backup to restore to
- UI shows status of backup verfication
- UI/API can be secured via OAuth openID


## UI
The UI is available via localhost:8081/
It shows a list of available full backups in S3. Any full backup contains 1 or more incremental backups, which can be selected to perform a complete restore!\
The color of an incremental backups shows the state of the backup verfication:\
```diff
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
Therefore the binlog needs to be enabled in the mariadb config
```
log-bin=bin.log      # Binlog folder and name
binlog_format=MIXED  # Formant, described below
expire_logs_days=3   # After x days binlog files get purged. Important! Otherwise volume could be filling up fast
server_id=1          # Unique server id. Used for replication
```

## Binlog
The tool acts like a replication slave and recieves all the binlog events. This allows it to check if any real changes have been made to the dabase.\
If no changes have been detected, no incremental backup will be created and saved to S3.


## Binlog Format
By default Mariadb will use the MIXED format (since 10.2.4). It is a mix of ROW and STATEMENT.\
ROW will capture the actual change made to a table. The binlog files therefor can get very large.\
e.g. an update to a table column of 1000 rows will create 1000 row changes. 
With STATEMENT only the update statement will be recorded in the binlog.


## Config
`service_name:` Name of your mariadb (also used as the s3 folder name)\
`namespace:` k8s namespace name\
`backup_service:`\
 &ensp;`full_backup_interval_in_hours:` Interval for full mariadb dumps in hours\
 &ensp;`incremental_backup_interval_in_minutes:` Interval for saving incremental backups\
 &ensp;`enable_init_restore:` Enables a automatic restore if one of the databases (in mariadb.databases) are missing.\
 &ensp;`enable_restore_on_db_failure`: Enables automatic restoe if the db is unhealthy.\
 &ensp;`outh:`\
    &emsp;`enabled:` enables oauth to access the API (openID)\
    &emsp;`provider_url:` Url of the openID provider (e.g. Dex)\
    &emsp;`redirect_url:` oauth redirect url (this is the url of your mariabackup service)\
 &ensp;`maria_db:` mariadb config\
    &emsp;`user:` user with admin rights (to drop and restart mariabd)\
    &emsp;`version:` mariadb version e.g.: "10.4.0"\
    &emsp;`password:` user password\
    &emsp;`host:` host of the mariabd instance. If running as a sidecar within the mariadb pod: 127.0.0.1\
    &emsp;`port:` mariadb port number\
    &emsp;`data_dir:` data directory of the mariadb instance\
    &emsp;`databases:` list of databases (used for healthy checks and restores)\
       &ensp;&emsp;- database_name\
       &ensp;&emsp;- ... \
     &emsp;`verify_tables:` list of tables for the backup verification check. If none are provided the checksum verficiation is skipped!\
       &ensp;&emsp;- database_name.table_name\
       &ensp;&emsp;- ... \
`storage_services:`\
  &ensp;`s3:`\
    &emsp;- `name:` name of the storage\
    &emsp;`aws_access_key_id:` s3 acces key\
    &emsp;`aws_secret_access_key:` s3 secret access key\
    &emsp;`region:` s3 region\
    &emsp;`bucket_name:` bucket name to save the backup to\
  &ensp;`swift:`\
    &emsp;- `name:` name of the storage\
    &emsp;`auth_version:` openstack auth version\
    &emsp;`auth_url:` openstack auth url (keystone url)\
    &emsp;`user_name:` os user name\
    &emsp;`user_domain_name:` os user domain name\
    &emsp;`project_name:` os project name\
    &emsp;`project_domain_name:` os project domain name\
    &emsp;`password:` os password\
    &emsp;`region:` region name\
    &emsp;`container_name:` name of the container the backups should be store in\
