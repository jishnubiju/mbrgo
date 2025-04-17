package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

func main() {
	log.Print("service started...")

	if err := godotenv.Load(); err != nil {
		log.Fatal("error loading .env file: ", err)
	}

	mysqlDB, err := initDb()
	if err != nil {
		log.Fatal("failed to initialize DB: ", err)
	}

	if err := mysqlDB.Validate(); err != nil {
		log.Fatal("invalid DB configuration: ", err)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema", mysqlDB.User, mysqlDB.Password, mysqlDB.Host, mysqlDB.Port)
	dbConn, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to connect to MySQL: %v", err)
	}
	defer dbConn.Close()

	cliArgs := os.Args[1:]
	if err := CliArgHandler(cliArgs, mysqlDB, dbConn); err != nil {
		log.Fatalf("error handling cli arguments: %v", err)
	}
}

func initDb() (*DB, error) {
	mysqlDB := &DB{
		Host:     os.Getenv("MYSQL_HOST"),
		User:     os.Getenv("MYSQL_USER"),
		Password: os.Getenv("MYSQL_PASSWORD"),
	}

	portStr := os.Getenv("MYSQL_PORT")
	if portStr != "" {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("error parsing MYSQL_PORT: %v", err)
		}
		mysqlDB.Port = port
	}

	return mysqlDB, nil
}

func CliArgHandler(cliArgs []string, mysqlDB *DB, dbConn *sql.DB) error {
	if len(cliArgs) < 1 {
		return fmt.Errorf("invalid argument, one of backup or restore must be provided")
	}

	switch cliArgs[0] {
	case "backup":
		if err := backupCli(cliArgs, mysqlDB, dbConn); err != nil {
			return fmt.Errorf("database backup failed: %w", err)
		}
	case "restore":
		if err := restoreCli(cliArgs, mysqlDB); err != nil {
			return fmt.Errorf("database restore failed: %w", err)
		}
	case "incremental-backup":
		if err := incrementalBackupCli(cliArgs, mysqlDB); err != nil {
			return fmt.Errorf("incremental backup failed: %w", err)
		}
	case "enable-all-backup-scheduler":
		if err := allBacupCli(cliArgs, mysqlDB, dbConn); err != nil {
			return fmt.Errorf("enable all backup scheduler failed: %w", err)
		}
	default:
		return fmt.Errorf("invalid command: %s, should be one of backup, restore, incremental-backup, enable-all-backup-scheduler", cliArgs[0])
	}
	return nil
}

func backupCli(cliArgs []string, mysqlDB *DB, dbConn *sql.DB) error {
	if len(cliArgs) < 2 {
		return fmt.Errorf("for backup, one of the all-database-full-backup, database=db_name, or databases=db1,db2,db3 must be provided")
	}

	var backupLocalDir string
	for _, arg := range cliArgs[1:] {
		if strings.HasPrefix(arg, "backup-local-dir=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) == 2 {
				backupLocalDir = parts[1]
			}
		}
	}

	if backupLocalDir == "" {
		return fmt.Errorf("for backup, backup-local-dir must be provided (e.g., backup-local-dir=your/path)")
	}

	arg := cliArgs[1]
	switch {
	case arg == "all-database-full-backup":
		// All databases full backup
		if err := mysqlDB.MysqlBackup(dbConn, true, "", nil, backupLocalDir); err != nil {
			return fmt.Errorf("all database full backup failed: %w", err)
		}
	case strings.HasPrefix(arg, "database="):
		// Single database full backup
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 || parts[1] == "" {
			return fmt.Errorf("invalid argument for single database backup. Usage: database=db_name")
		}
		database := parts[1]
		if err := mysqlDB.MysqlBackup(dbConn, false, database, nil, backupLocalDir); err != nil {
			return fmt.Errorf("database full backup failed: %w", err)
		}
	case strings.HasPrefix(arg, "databases="):
		// Multiple databases full backup
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 || parts[1] == "" {
			return fmt.Errorf("invalid argument for multiple databases backup. Usage: databases=db1,db2,db3")
		}
		dbList := strings.Split(parts[1], ",")
		cleanedDbList := []string{}
		for _, database := range dbList {
			cleanedDatabase := strings.Trim(database, " ")
			cleanedDbList = append(cleanedDbList, cleanedDatabase)
		}
		if err := mysqlDB.MysqlBackup(dbConn, false, "", cleanedDbList, backupLocalDir); err != nil {
			return fmt.Errorf("multiple databases full backup failed: %w", err)
		}
	default:
		return fmt.Errorf("unknown backup type: %s", arg)
	}
	return nil
}

func restoreCli(cliArgs []string, mysqlDB *DB) error {
	var backupS3Dir, restoreDir string
	for _, arg := range cliArgs[1:] {
		if strings.HasPrefix(arg, "backup-s3-dir=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) == 2 {
				backupS3Dir = parts[1]
			}
		} else if strings.HasPrefix(arg, "restore-dir=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) == 2 {
				restoreDir = parts[1]
			}
		}
	}
	if backupS3Dir == "" || restoreDir == "" {
		return fmt.Errorf("for restore, both backup-s3-dir and restore-dir must be provided (e.g., backup-s3-dir=your/s3/path restore-dir=/your/restore/path)")
	}
	arg := cliArgs[1]
	switch {
	case arg == "all-database-full-restore":
		if err := mysqlDB.MysqlRestore(backupS3Dir, restoreDir, true, "", nil); err != nil {
			return fmt.Errorf("all database restore failed: %w", err)
		}
	case strings.HasPrefix(arg, "database="):
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 || parts[1] == "" {
			return fmt.Errorf("invalid argument for single database restore. Usage: database=db_name")
		}
		database := parts[1]
		if err := mysqlDB.MysqlRestore(backupS3Dir, restoreDir, false, database, nil); err != nil {
			return fmt.Errorf("restore failed: %w", err)
		}
	case strings.HasPrefix(arg, "databases="):
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 || parts[1] == "" {
			return fmt.Errorf("invalid argument for multiple databases restore. Usage: databases=db1,db2,db3")
		}
		dbList := strings.Split(parts[1], ",")
		if err := mysqlDB.MysqlRestore(backupS3Dir, restoreDir, false, "", dbList); err != nil {
			return fmt.Errorf("restore failed: %w", err)
		}
	default:
		return fmt.Errorf("unknown restore type: %s", arg)
	}
	return nil
}

func allBacupCli(cliArgs []string, mysqlDB *DB, dbConn *sql.DB) error {
	var weekday, hourStr, backupLocalDir string
	for _, arg := range cliArgs[1:] {
		if strings.HasPrefix(arg, "weekday=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) == 2 {
				weekday = parts[1]
			}
		} else if strings.HasPrefix(arg, "hour=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) == 2 {
				hourStr = parts[1]
			}
		} else if strings.HasPrefix(arg, "backup-local-dir=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) == 2 {
				backupLocalDir = parts[1]
			}
		}
	}

	if weekday == "" || hourStr == "" || backupLocalDir == "" {
		return fmt.Errorf("for enable-all-backup-scheduler, both weekday, hour and backup-local-dir must be provided (e.g., weekday=Mon hour=00:00 backup-local-dir=your/path)")
	}
	log.Printf("enabling backup scheduler every %s at %s", weekday, hourStr)
	if err := mysqlDB.EnableAllBackupScheduler(dbConn, weekday, hourStr, backupLocalDir); err != nil {
		return fmt.Errorf("failed to enable backup scheduler: %v", err)
	}
	select {}
}

func incrementalBackupCli(cliArgs []string, mysqlDB *DB) error {
	var backupLocalDir string
	for _, arg := range cliArgs[1:] {
		if strings.HasPrefix(arg, "backup-local-dir=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) == 2 {
				backupLocalDir = parts[1]
			}
		}
	}

	if backupLocalDir == "" {
		return fmt.Errorf("for backup, backup-local-dir must be provided (e.g., backup-local-dir=your/path)")
	}

	if err := mysqlDB.MysqlIncrementalBackup(context.Background(), backupLocalDir); err != nil {
		return fmt.Errorf("incremental backup failed: %w", err)
	}
	return nil
}
