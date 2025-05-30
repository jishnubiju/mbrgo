# mbrgo

Mysql backup restore service in Golang

## Overview

`mbrgo` is a service written in Go that provides functionalities to perform full and incremental backups of MySQL databases and restore them from backups. The service supports uploading backups to AWS S3 and downloading them for restoration.

## Features

- Full backup of all databases or specific databases.
- Incremental backup using MySQL binlog.
- Upload backups to AWS S3.
- Download backups from AWS S3.
- Restore databases from full and incremental backups.
- Schedule backups at a specified time.

## Environment Variables

The service relies on the following environment variables:

- `MYSQL_HOST`: MySQL server host.
- `MYSQL_PORT`: MySQL server port.
- `MYSQL_USER`: MySQL user.
- `MYSQL_PASSWORD`: MySQL user password.
- `MYSQL_BACKUP_PATH`: Local path to store backups.
- `AWS_S3_BUCKET`: AWS S3 bucket name for storing backups.

## Usage

### Full Backup

The `MysqlBackup` function performs a full backup of the specified databases or all databases if `AllDatabases` is set to true. The backup files are stored locally and uploaded to AWS S3.

### Incremental Backup

The `MysqlIncrementalBackup` function performs an incremental backup using MySQL binlog. The binlog events are streamed and stored locally, and the backup files are uploaded to AWS S3.

### Restore

The `MysqlRestore` function restores databases from full and incremental backups. The backups are downloaded from AWS S3 and restored to the MySQL server.

### Schedule Backup

The `EnableAllBackupScheduler` function schedules full and incremental backups at a specified time every week.

## CLI Usage

The service can be started with the following CLI arguments:

### Backup

- **All Databases Full Backup**: `backup all-database-full-backup backup-local-dir=<your/path>`
- **Single Database Full Backup**: `backup database=<db_name> backup-local-dir=<your/path>`
- **Multiple Databases Full Backup**: `backup databases=<db1,db2,db3> backup-local-dir=<your/path>`

### Restore

- **All Databases Full Restore**: `restore all-database-full-restore backup-s3-dir=<your/s3/path> restore-dir=<your/restore/path>`
- **Single Database Full Restore**: `restore database=<db_name> backup-s3-dir=<your/s3/path> restore-dir=<your/restore/path>`
- **Multiple Databases Full Restore**: `restore databases=<db1,db2,db3> backup-s3-dir=<your/s3/path> restore-dir=<your/restore/path>`

### Incremental Backup

- **Incremental Backup**: `incremental-backup backup-local-dir=<your/path>`

### Schedule Backup

- **Enable All Backup Scheduler**: `enable-all-backup-scheduler weekday=<weekday> hour=<hour> backup-local-dir=<your/path>`

## Functions

### `main.go`

- `main()`: Entry point of the service. Initializes the database connection and handles CLI arguments.
- `initDb()`: Initializes the database configuration from environment variables.
- `CliArgHandler(cliArgs []string, mysqlDB *DB, dbConn *sql.DB)`: Handles command-line arguments for backup and restore operations.

### `model.go`

- `DB`: Struct holding the configuration for the database connection and backup settings.
- `Validate()`: Validates the `DB` struct fields.

### `upload.go`

- `StreamBinlogToS3(data []byte, fileName string)`: Streams binlog data to AWS S3.
- `UploadBufferToS3(data []byte, fileName string)`: Uploads a buffer to AWS S3.
- `getS3Key(fileName string)`: Generates the S3 key for the backup file.
- `getStreamS3Key(fileName string)`: Generates the S3 key for the binlog stream.

### `download.go`

- `s3Download(backupS3Dir string, restorePath string)`: Downloads backups from AWS S3.
- `downloadFile(ctx context.Context, downloader *manager.Downloader, bucket, key, destFile string)`: Downloads a file from AWS S3.

### `backup.go`

- `MysqlBackup(dbConn *sql.DB, allDBFull bool, database string, databases []string, backupDir string)`: Performs a full backup of the specified databases or all databases.
- `backupAllDatabases(db *DB, backupFile string)`: Backs up all databases.
- `singleDbBackup(db *DB, database string, backupFile string, dbConn *sql.DB, backupFileName string)`: Backs up a single database.
- `uploadBackupToS3(backupFile, backupFileName string)`: Uploads the backup file to AWS S3.
- `databaseExists(db *sql.DB, dbName string)`: Checks if a database exists.
- `saveCurrentBinlogPosition(db *sql.DB, metadataFile string)`: Saves the current binlog position.
- `backupError(err error, database string, output []byte)`: Handles backup errors.

### `incremental_backup.go`

- `MysqlIncrementalBackup(ctx context.Context, backupDir string)`: Performs an incremental backup using MySQL binlog.
- `openNewFile(dirPath string)`: Opens a new file for storing binlog events.
- `streamData(ctx context.Context, streamer *replication.BinlogStreamer, dirPath string)`: Streams binlog events to a file.
- `processEvent(ev *replication.BinlogEvent, currentFile *os.File, dirPath string)`: Processes a binlog event.
- `writeBufferToFile(currentFile *os.File)`: Writes the buffer to the current file.
- `rotateFile(file *os.File, dirPath string)`: Rotates the current file.
- `getLastBinlogPosition(metadataFile string)`: Gets the last binlog position from the metadata file.

### `restore.go`

- `MysqlRestore(backupS3Dir string, restoreDir string, allDBFull bool, database string, databases []string)`: Restores databases from full and incremental backups.
- `findFullBackupFile(restorePath, database string)`: Finds the full backup file for a database.
- `restoreFullBackup(db *DB, backupFile string, targetDatabase string)`: Restores a full backup.
- `restoreIncrementalBackup(db *DB, restorePath string)`: Restores incremental backups.
- `restoreFromRawBinlog(db *DB, backupFile string)`: Restores from raw binlog.

### `schedule.go`

- `EnableAllBackupScheduler(dbConn *sql.DB, weekday string, hour string, backupLocalDir string)`: Schedules full and incremental backups at a specified time every week.
- `scheduleBackup(db *DB, rootCtx context.Context, dbConn *sql.DB, weekday time.Weekday, hour time.Time, incCancel *context.CancelFunc, backupLocalDir string)`: Schedules the backup.
- `backup(db *DB, rootCtx context.Context, dbConn *sql.DB, incCancel *context.CancelFunc, backupLocalDir string)`: Performs the full and incremental backups.
- `parseWeekday(weekday string) (time.Weekday, error)`: Parses the weekday string to a `time.Weekday`.

## License

This project is licensed under the MIT License.