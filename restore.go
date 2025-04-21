package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// MysqlRestore restores MySQL databases from backups stored in an S3 bucket.
//
// Parameters:
// - backupS3Dir: The S3 directory (prefix) containing the backup files.
// - restoreDir: The local directory where the backups will be downloaded and restored from.
// - allDBFull: A boolean indicating whether to restore all databases.
// - database: The name of a single database to restore (if specified).
// - databases: A list of database names to restore (if specified).
//
// Returns:
// - error: An error if the restore process fails, otherwise nil.
func (db *DB) MysqlRestore(backupS3Dir string, restoreDir string, allDBFull bool, database string, databases []string) error {
	log.Print("mysql restore function started..!")

	// Download backup files from S3 to the local restore directory.
	if err := s3Download(backupS3Dir, restoreDir); err != nil {
		return fmt.Errorf("failed to download from S3: %w", err)
	}

	if allDBFull {
		log.Print("restoring all databases..!")
		backupFile, err := findFullBackupFile(restoreDir, "")
		if err != nil {
			return fmt.Errorf("error finding full backup for all databases: %w", err)
		}
		if err := restoreFullBackup(db, backupFile, ""); err != nil {
			return fmt.Errorf("failed to restore full backup for all databases: %w", err)
		}
		if err := restoreIncrementalBackup(db, restoreDir); err != nil {
			return fmt.Errorf("failed to restore incremental backup: %w", err)
		}
		log.Print("Restore all databases completed..!")
	} else {
		if databases != nil {
			for _, database := range databases {
				log.Printf("Restoring database: %s", database)
				backupFile, err := findFullBackupFile(restoreDir, database)
				if err != nil {
					log.Printf("Error finding full backup for database %s: %v", database, err)
					continue
				}
				if err := restoreFullBackup(db, backupFile, database); err != nil {
					log.Printf("failed to restore full backup for database %s: %v", database, err)
				}
			}
		}
		if database != "" {
			log.Printf("Restoring database: %s", database)
			backupFile, err := findFullBackupFile(restoreDir, database)
			if err != nil {
				log.Printf("Error finding full backup for database %s: %v", database, err)
			} else {
				if err := restoreFullBackup(db, backupFile, database); err != nil {
					log.Printf("failed to restore full backup for database %s: %v", database, err)
				}
			}
		}
		if err := restoreIncrementalBackup(db, restoreDir); err != nil {
			return fmt.Errorf("failed to restore incremental backup: %w", err)
		}
	}
	log.Print("mysql restore function finished..!")
	return nil
}

// findFullBackupFile locates the full backup file for a specific database or all databases.
//
// Parameters:
// - restorePath: The local directory where the backup files are stored.
// - database: The name of the database to find the backup for (empty for all databases).
//
// Returns:
// - string: The path to the full backup file.
// - error: An error if the backup file is not found or the directory cannot be read.
func findFullBackupFile(restorePath, database string) (string, error) {
	entries, err := os.ReadDir(restorePath)
	if err != nil {
		return "", err
	}

	var pattern string
	if database == "" {
		pattern = "all_databases_full_backup.sql"
	} else {
		// For individual database backups.
		pattern = fmt.Sprintf("%s_full_backup.sql", database)
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), pattern) {
			return filepath.Join(restorePath, entry.Name()), nil
		}
	}
	return "", fmt.Errorf("backup file not found for pattern: %s", pattern)
}

// restoreFullBackup restores a full backup for a specific database or all databases.
//
// Parameters:
// - db: The database configuration object.
// - backupFile: The path to the full backup file.
// - targetDatabase: The name of the database to restore (empty for all databases).
//
// Returns:
// - error: An error if the restore process fails, otherwise nil.
func restoreFullBackup(db *DB, backupFile string, targetDatabase string) error {
	var commandStr string
	if targetDatabase == "" {
		commandStr = fmt.Sprintf("mysql --host %s --port %d --user %s --password=%s < %s",
			db.Host, db.Port, db.User, db.Password, backupFile)
	} else {
		commandStr = fmt.Sprintf("mysql --host %s --port %d --user %s --password=%s %s < %s",
			db.Host, db.Port, db.User, db.Password, targetDatabase, backupFile)
	}

	command := exec.Command("sh", "-c", commandStr)
	output, err := command.CombinedOutput()
	if err != nil {
		if targetDatabase == "" {
			restoreError(err, "all databases", output)
		} else {
			restoreError(err, targetDatabase, output)
		}
		return err
	} else {
		if targetDatabase == "" {
			log.Printf("restore of all databases completed successfully")
		} else {
			log.Printf("restore of database %s completed successfully", targetDatabase)
		}
	}
	return nil
}

// restoreError logs detailed information about a restore error.
//
// Parameters:
// - err: The error object.
// - database: The name of the database being restored.
// - output: The output from the restore command.
func restoreError(err error, database string, output []byte) {
	if exitError, ok := err.(*exec.ExitError); ok {
		exitCode := exitError.ExitCode()
		if exitCode == 2 {
			log.Printf("%s restore completed with warning (exit code %d): output: %s", database, exitCode, output)
		} else {
			log.Printf("%s restore failed with exit code %d: error: %v, output: %s", database, exitCode, err, output)
		}
	} else {
		log.Printf("%s restore failed: %v, output: %s", database, err, output)
	}
}

// restoreIncrementalBackup restores incremental backups from binary logs.
//
// Parameters:
// - db: The database configuration object.
// - restorePath: The local directory where the incremental backups are stored.
//
// Returns:
// - error: An error if the restore process fails, otherwise nil.
func restoreIncrementalBackup(db *DB, restorePath string) error {
	log.Print("mysql restore incremental backup function started..!")

	weeklyBinlogPath := filepath.Join(restorePath, "weekly-binlog.log")
	if _, err := os.Stat(weeklyBinlogPath); err == nil {
		log.Printf("Restoring binlog from weekly-binlog.log: %s", weeklyBinlogPath)
		if err := restoreFromRawBinlog(db, weeklyBinlogPath); err != nil {
			return fmt.Errorf("failed to restore from weekly binlog: %w", err)
		}
	} else {
		log.Printf("weekly-binlog.log not found in backup directory: %s", restorePath)
	}
	return nil
}

// restoreFromRawBinlog restores data from a raw binary log file.
//
// Parameters:
// - db: The database configuration object.
// - backupFile: The path to the binary log file.
//
// Returns:
// - error: An error if the restore process fails, otherwise nil.
func restoreFromRawBinlog(db *DB, backupFile string) error {
	commandStr := fmt.Sprintf("mysqlbinlog --host=%s --port=%d --user=%s --password=%s %s | mysql --host=%s --port=%d --user=%s --password=%s",
		db.Host, db.Port, db.User, db.Password, backupFile,
		db.Host, db.Port, db.User, db.Password)
	command := exec.Command("sh", "-c", commandStr)
	output, err := command.CombinedOutput()
	if err != nil {
		log.Printf("failed to restore from binlog: %v, output: %s", err, output)
		return err
	} else {
		log.Print("restore from binlog completed successfully")
	}
	return nil
}
