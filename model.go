package main

import "fmt"

// DB holds the configuration for the database connection and backup settings.
//
// Fields:
// - Host: The database server host (e.g., "localhost" or an IP address).
// - User: The database user with sufficient privileges for backup and restore operations.
// - Password: The password for the database user.
// - Database: The name of a single database to connect to (optional if AllDatabases is true).
// - Databases: A list of specific databases to back up (optional if AllDatabases is true).
// - AllDatabases: A boolean indicating whether to back up all databases.
// - Port: The port number on which the database server is running (e.g., 3306 for MySQL).
type DB struct {
	Host         string
	User         string
	Password     string
	Database     string
	Databases    []string
	AllDatabases bool
	Port         int
}

// Validate checks if the DB struct has valid values.
//
// Returns:
// - error: An error if any required field is missing or invalid, otherwise nil.
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
