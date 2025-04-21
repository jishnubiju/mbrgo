package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

// EnableAllBackupScheduler enables a backup scheduler for MySQL databases.
// It schedules full and incremental backups to run at a specified weekday and time.
//
// Parameters:
// - dbConn: The database connection object.
// - weekday: The day of the week when the backup should run (e.g., "Monday").
// - hour: The time of day when the backup should run (in "HH:MM" format).
// - backupLocalDir: The local directory where backups will be stored.
//
// Returns:
// - error: An error if the scheduler setup fails, otherwise nil.
func (db *DB) EnableAllBackupScheduler(dbConn *sql.DB, weekday string, hour string, backupLocalDir string) error {
	weekdayTime, err := parseWeekday(weekday)
	if err != nil {
		return fmt.Errorf("invalid weekday: %v", err)
	}

	hourTime, err := time.Parse("15:04", hour)
	if err != nil {
		return fmt.Errorf("invalid hour: %v", err)
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	var incCancel context.CancelFunc

	go scheduleBackup(db, rootCtx, dbConn, weekdayTime, hourTime, &incCancel, backupLocalDir)
	select {}
}

// scheduleBackup schedules full and incremental backups to run periodically.
//
// Parameters:
// - db: The database configuration object.
// - rootCtx: The root context for managing cancellations.
// - dbConn: The database connection object.
// - weekday: The day of the week when the backup should run.
// - hour: The time of day when the backup should run.
// - incCancel: A pointer to a context cancel function for incremental backups.
// - backupLocalDir: The local directory where backups will be stored.
func scheduleBackup(db *DB, rootCtx context.Context, dbConn *sql.DB, weekday time.Weekday, hour time.Time, incCancel *context.CancelFunc, backupLocalDir string) {
	now := time.Now()
	nextBackup := time.Date(now.Year(), now.Month(), now.Day(), hour.Hour(), hour.Minute(), 0, 0, now.Location())
	if now.After(nextBackup) || now.Weekday() != weekday {
		daysToAdd := (int(weekday) - int(now.Weekday()) + 7) % 7
		if daysToAdd == 0 || now.After(nextBackup) {
			daysToAdd = 7
		}
		nextBackup = nextBackup.AddDate(0, 0, daysToAdd)
		log.Printf("first backup scheduled at %s", nextBackup.Format(time.RFC1123))
	}
	duration := time.Until(nextBackup)
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		log.Println("timer expired, scheduling backup...")
		backup(db, rootCtx, dbConn, incCancel, backupLocalDir)
		ticker := time.NewTicker(7 * 24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				backup(db, rootCtx, dbConn, incCancel, backupLocalDir)
			case <-rootCtx.Done():
				log.Println("root context cancelled, stopping backup scheduler")
				return
			}
		}
	case <-rootCtx.Done():
		log.Println("root context cancelled, stopping backup scheduler")
		timer.Stop()
	}
}

// backup performs a full backup and schedules an incremental backup.
//
// Parameters:
// - db: The database configuration object.
// - rootCtx: The root context for managing cancellations.
// - dbConn: The database connection object.
// - incCancel: A pointer to a context cancel function for incremental backups.
// - backupLocalDir: The local directory where backups will be stored.
func backup(db *DB, rootCtx context.Context, dbConn *sql.DB, incCancel *context.CancelFunc, backupLocalDir string) {
	log.Printf("backup taken at %s", time.Now().Format(time.RFC1123))
	if err := db.MysqlBackup(dbConn, true, "", nil, backupLocalDir); err != nil {
		log.Printf("Error during full backup: %v", err)
	}

	if *incCancel != nil {
		(*incCancel)()
	}
	incCtx, cancelFunc := context.WithCancel(rootCtx)
	*incCancel = cancelFunc

	go func(ctx context.Context) {
		log.Printf("Incremental backup taken at %s", time.Now().Format(time.RFC1123))
		if err := db.MysqlIncrementalBackup(ctx, backupLocalDir); err != nil {
			log.Printf("Error during incremental backup at %s: %v", time.Now().Format(time.RFC1123), err)
		}
	}(incCtx)

}

// parseWeekday parses a string representation of a weekday into a time.Weekday value.
//
// Parameters:
// - weekday: The string representation of the weekday (e.g., "Monday").
//
// Returns:
// - time.Weekday: The parsed weekday value.
// - error: An error if the weekday string is invalid.
func parseWeekday(weekday string) (time.Weekday, error) {
	weekdays := map[string]time.Weekday{
		"sunday":    time.Sunday,
		"monday":    time.Monday,
		"tuesday":   time.Tuesday,
		"wednesday": time.Wednesday,
		"thursday":  time.Thursday,
		"friday":    time.Friday,
		"saturday":  time.Saturday,
	}

	lowerWeekday := stringToLower(weekday)
	if wd, ok := weekdays[lowerWeekday]; ok {
		return wd, nil
	}
	return time.Sunday, fmt.Errorf("invalid weekday: %s", weekday)
}

// stringToLower normalizes a string representation of a weekday to lowercase.
//
// Parameters:
// - s: The string representation of the weekday.
//
// Returns:
// - string: The normalized lowercase representation of the weekday.
func stringToLower(s string) string {
	return map[string]string{
		"Sun": "sunday", "sun": "sunday", "Sunday": "sunday", "sunday": "sunday",
		"Mon": "monday", "mon": "monday", "Monday": "monday", "monday": "monday",
		"Tue": "tuesday", "tue": "tuesday", "Tuesday": "tuesday", "tuesday": "tuesday",
		"Wed": "wednesday", "wed": "wednesday", "Wednesday": "wednesday", "wednesday": "wednesday",
		"Thu": "thursday", "thu": "thursday", "Thursday": "thursday", "thursday": "thursday",
		"Fri": "friday", "fri": "friday", "Friday": "friday", "friday": "friday",
		"Sat": "saturday", "sat": "saturday", "Saturday": "saturday", "saturday": "saturday",
	}[s]
}
