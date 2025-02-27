package main

import "fmt"

// DB holds the configuration for the database connection and backup settings.
type DB struct {
	Host         string   // Host is the database server host.
	User         string   // User is the database user.
	Password     string   // Password is the database user's password.
	Database     string   // Database is the name of the database to connect to.
	Databases    []string // Databases is a list of databases to back up.
	AllDatabases bool     // AllDatabases indicates whether to back up all databases.
	Port         int      // Port is the database server port.
}

// Validate checks if the DB struct has valid values.
func (db *DB) Validate() error {
	if db.Host == "" {
		return fmt.Errorf("host is required")
	}
	if db.User == "" {
		return fmt.Errorf("user is required")
	}
	if db.Password == "" {
		return fmt.Errorf("password is required")
	}
	if db.Port <= 0 {
		return fmt.Errorf("port must be a positive integer")
	}
	return nil
}
