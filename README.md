# maria-back-me-up
Mariadb backup tool


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