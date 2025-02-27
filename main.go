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
	log.Print("Service started...")

	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file: ", err)
	}

	mysqlDB, err := initializeDB()
	if err != nil {
		log.Fatal("Failed to initialize DB: ", err)
	}

	if err := mysqlDB.Validate(); err != nil {
		log.Fatal("Invalid DB configuration: ", err)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema", mysqlDB.User, mysqlDB.Password, mysqlDB.Host, mysqlDB.Port)
	dbConn, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer dbConn.Close()

	cliArgs := os.Args[1:]
	if err := CliArgHandler(cliArgs, mysqlDB, dbConn); err != nil {
		log.Fatalf("Error handling CLI arguments: %v", err)
	}
}

func initializeDB() (*DB, error) {
	mysqlDB := &DB{
		Host:     getEnv("MYSQL_HOST"),
		User:     getEnv("MYSQL_USER"),
		Password: getEnv("MYSQL_PASSWORD"),
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

func getEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is not set", key)
	}
	return value
}

func CliArgHandler(cliArgs []string, mysqlDB *DB, dbConn *sql.DB) error {
	if len(cliArgs) < 1 {
		return fmt.Errorf("invalid argument, one of backup or restore must be provided")
	}

	switch cliArgs[0] {
	case "backup":
		if len(cliArgs) < 2 {
			return fmt.Errorf("for backup, one of the all-database-full-backup, database=db_name, or databases=db1,db2,db3 must be provided")
		}
		arg := cliArgs[1]
		switch {
		case arg == "all-database-full-backup":
			// All databases full backup
			if err := mysqlDB.MysqlFullBackup(dbConn, true, "", nil); err != nil {
				return fmt.Errorf("all-database full backup failed: %w", err)
			}
		case strings.HasPrefix(arg, "database="):
			// Single database full backup
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) != 2 || parts[1] == "" {
				return fmt.Errorf("invalid argument for single database backup. Usage: database=db_name")
			}
			database := parts[1]
			if err := mysqlDB.MysqlFullBackup(dbConn, false, database, nil); err != nil {
				return fmt.Errorf("database full backup failed: %w", err)
			}
		case strings.HasPrefix(arg, "databases="):
			// Multiple databases full backup
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) != 2 || parts[1] == "" {
				return fmt.Errorf("invalid argument for multiple databases backup. Usage: databases=db1,db2,db3")
			}
			dbList := strings.Split(parts[1], ",")
			if err := mysqlDB.MysqlFullBackup(dbConn, false, "", dbList); err != nil {
				return fmt.Errorf("multiple databases full backup failed: %w", err)
			}
		default:
			return fmt.Errorf("unknown backup type: %s", arg)
		}
	case "restore":
		// Expect restore command to have backupS3Dir and restoreDir arguments.
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
				return fmt.Errorf("restore failed: %w", err)
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
	case "incremental-backup":
		if err := mysqlDB.MysqlIncrementalBackup(context.Background()); err != nil {
			return fmt.Errorf("incremental backup failed: %w", err)
		}
	case "enable-all-backup-scheduler":
		var weekday, hourStr string
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
			}
		}
		if weekday == "" || hourStr == "" {
			log.Fatal("for enable-all-backup-scheduler, both weekday and hour must be provided (e.g., weekday=Mon hour=00:00)")
		}
		log.Printf("Enabling backup scheduler every %s at %s", weekday, hourStr)
		if err := mysqlDB.EnableAllBackupScheduler(dbConn, weekday, hourStr); err != nil {
			log.Fatalf("Failed to enable backup scheduler: %v", err)
		}
		select {}
	default:
		return fmt.Errorf("invalid command: %s, should be one of backup, restore, incremental-backup, enable-all-backup-scheduler", cliArgs[0])
	}
	return nil
}
