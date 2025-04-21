package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"
)

// MysqlBackup performs a MySQL backup operation.
// It supports both full backups of all databases and backups of specific databases.
//
// Parameters:
// - dbConn: The database connection object.
// - allDBFull: A boolean indicating whether to back up all databases.
// - database: The name of a single database to back up (if specified).
// - databases: A list of database names to back up (if specified).
// - backupDir: The directory where the backup files will be stored.
//
// Returns:
// - error: An error if the backup or upload process fails, otherwise nil.
func (db *DB) MysqlBackup(dbConn *sql.DB, allDBFull bool, database string, databases []string, backupDir string) error {
	log.Print("mysql full backup function started..!")

	binlogMetadataFile := fmt.Sprintf("%s/binlog_position.txt", backupDir)

	if allDBFull {
		backupFileName := fmt.Sprintf("%s_all_databases_full_backup.sql", time.Now().Format("20060102_150405"))
		backupFile := fmt.Sprintf("%s/%s", backupDir, backupFileName)
		if err := backupAllDatabases(db, backupFile); err != nil {
			return fmt.Errorf("failed to backup all databases: %w", err)
		}
		saveCurrentBinlogPosition(dbConn, binlogMetadataFile)
		if err := uploadBackupToS3(backupFile, backupFileName); err != nil {
			return fmt.Errorf("failed to upload backup to S3: %w", err)
		}
		log.Print("backup all databases completed..!")
	} else {
		if databases != nil {
			for _, database := range databases {
				backupFileName := fmt.Sprintf("%s_%s_full_backup.sql", time.Now().Format("20060102_150405"), database)
				backupFile := fmt.Sprintf("%s/%s", backupDir, backupFileName)
				if err := singleDbBackup(db, database, backupFile, dbConn, backupFileName); err != nil {
					log.Printf("Failed to backup database %s: %v", database, err)
				}
			}
		} else if database != "" {
			backupFileName := fmt.Sprintf("%s_%s_full_backup.sql", time.Now().Format("20060102_150405"), database)
			backupFile := fmt.Sprintf("%s/%s", backupDir, backupFileName)
			if err := singleDbBackup(db, database, backupFile, dbConn, backupFileName); err != nil {
				log.Printf("Failed to backup database %s: %v", database, err)
			}
		} else {
			return fmt.Errorf("no database specified for backup")
		}
	}
	log.Print("mysql full backup function finished..!")
	return nil
}

// backupAllDatabases performs a full backup of all databases.
//
// Parameters:
// - db: The database configuration object.
// - backupFile: The path to the file where the backup will be stored.
//
// Returns:
// - error: An error if the backup process fails, otherwise nil.
func backupAllDatabases(db *DB, backupFile string) error {
	commandStr := fmt.Sprintf("mysqldump --host %s --port %d --user %s --password=%s --all-databases --flush-logs --single-transaction > %s", db.Host, db.Port, db.User, db.Password, backupFile)
	command := exec.Command("sh", "-c", commandStr)
	output, err := command.CombinedOutput()
	if err != nil {
		backupError(err, "all databases", output)
		return err
	}
	log.Print("backup all databases completed..!")
	return nil
}

// singleDbBackup performs a backup of a single database.
//
// Parameters:
// - db: The database configuration object.
// - database: The name of the database to back up.
// - backupFile: The path to the file where the backup will be stored.
// - dbConn: The database connection object.
// - backupFileName: The name of the backup file.
//
// Returns:
// - error: An error if the backup or upload process fails, otherwise nil.
func singleDbBackup(db *DB, database string, backupFile string, dbConn *sql.DB, backupFileName string) error {
	ok, err := databaseExists(dbConn, database)
	if !ok {
		return fmt.Errorf("database %s does not exist: %v", database, err)
	}

	commandStr := fmt.Sprintf("mysqldump --host %s --port %d --user %s --password=%s --databases %s > %s", db.Host, db.Port, db.User, db.Password, database, backupFile)
	command := exec.Command("sh", "-c", commandStr)
	output, err := command.CombinedOutput()
	if err != nil {
		backupError(err, database, output)
		return err
	}

	if err := uploadBackupToS3(backupFile, backupFileName); err != nil {
		return fmt.Errorf("failed to upload backup to S3: %w", err)
	}

	log.Printf("backup %s completed..!", database)
	return nil
}

// uploadBackupToS3 uploads a backup file to an S3 bucket.
//
// Parameters:
// - backupFile: The path to the backup file.
// - backupFileName: The name of the backup file to be used as the S3 key.
//
// Returns:
// - error: An error if the upload process fails, otherwise nil.
func uploadBackupToS3(backupFile, backupFileName string) error {
	data, err := os.ReadFile(backupFile)
	if err != nil {
		return fmt.Errorf("error reading backup file: %w", err)
	}
	UploadBufferToS3(data, backupFileName)
	return nil
}

// databaseExists checks if a database exists in the MySQL server.
//
// Parameters:
// - db: The database connection object.
// - dbName: The name of the database to check.
//
// Returns:
// - bool: True if the database exists, otherwise false.
// - error: An error if the query fails.
func databaseExists(db *sql.DB, dbName string) (bool, error) {
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?)"
	err := db.QueryRow(query, dbName).Scan(&exists)
	return exists, err
}

// saveCurrentBinlogPosition saves the current binary log position to a metadata file.
//
// Parameters:
// - db: The database connection object.
// - metadataFile: The path to the metadata file where the binlog position will be saved.
func saveCurrentBinlogPosition(db *sql.DB, metadataFile string) {
	var binlogFile string
	var binlogPos uint32
	var dummy1, dummy2, dummy3 interface{}

	query := "SHOW MASTER STATUS"
	row := db.QueryRow(query)
	err := row.Scan(&binlogFile, &binlogPos, &dummy1, &dummy2, &dummy3)
	if err != nil {
		log.Printf("error fetching binlog position: %v", err)
		return
	}

	file, err := os.Create(metadataFile)
	if err != nil {
		log.Printf("error creating metadata file: %v", err)
		return
	}
	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("%s %d\n", binlogFile, binlogPos))
	if err != nil {
		log.Printf("Error writing to metadata file: %v", err)
		return
	}

	log.Printf("saved binlog position: %s at %d", binlogFile, binlogPos)
}

// backupError logs detailed information about a backup error.
//
// Parameters:
// - err: The error object.
// - database: The name of the database being backed up.
// - output: The output from the backup command.
func backupError(err error, database string, output []byte) {
	if exitError, ok := err.(*exec.ExitError); ok {
		exitCode := exitError.ExitCode()
		if exitCode == 2 {
			log.Printf("%s backup completed with warning (exit code %d): output: %s", database, exitCode, output)
		} else {
			log.Printf("%s backup failed with exit code %d: error: %v, output: %s", database, exitCode, err, output)
		}
	} else {
		log.Printf("%s backup failed: %v, output: %s", database, err, output)
	}
}
