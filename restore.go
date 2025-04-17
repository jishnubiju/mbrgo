package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func (db *DB) MysqlRestore(backupS3Dir string, restoreDir string, allDBFull bool, database string, databases []string) error {
	log.Print("mysql restore function started..!")

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
