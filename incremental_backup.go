package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	bufferSize  = 2 * 1024 * 1024  // Size of the buffer for binlog events.
	maxFileSize = 10 * 1024 * 1024 // Maximum size of a single backup file.
)

var (
	buffer        = make([]byte, 0, bufferSize) // Buffer to store binlog events.
	currentSize   int64                         // Current size of the backup file.
	fileIndex     = 0                           // Index for naming backup files.
	currentFile   *os.File                      // Current backup file being written to.
	currentBinlog = "binlog.000001"             // Current binlog file being processed.
)

// openNewFile creates a new backup file in the specified directory.
//
// Parameters:
// - dirPath: The directory where the new backup file will be created.
//
// Returns:
// - *os.File: The newly created file.
// - error: An error if the file creation fails.
func openNewFile(dirPath string) (*os.File, error) {
	filename := fmt.Sprintf("%s/incr_backup_%s_%d_%s.log", dirPath, currentBinlog, fileIndex, time.Now().Format("20060102_150405"))
	fileIndex++
	log.Printf("rotating to new file: %s", filename)
	return os.Create(filename)
}

// streamData streams binlog events from the MySQL server and writes them to backup files.
//
// Parameters:
// - ctx: The context for managing cancellations.
// - streamer: The binlog streamer instance.
// - dirPath: The directory where backup files will be stored.
func streamData(ctx context.Context, streamer *replication.BinlogStreamer, dirPath string) {
	log.Print("streaming data started...")
	var err error
	currentFile, err = openNewFile(dirPath)
	if err != nil {
		log.Fatalf("cannot create backup file: %v", err)
	}
	defer currentFile.Close()

	for {
		select {
		case <-ctx.Done():
			log.Println("incremental backup cancelled.")
			return
		default:
			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				log.Printf("error getting binlog event: %v", err)
				continue
			}
			log.Printf("received binlog event: %T", ev.Event)
			processEvent(ev, currentFile, dirPath)
		}
	}
}

// processEvent processes a single binlog event and writes it to the backup file.
//
// Parameters:
// - ev: The binlog event to process.
// - currentFile: The current backup file being written to.
// - dirPath: The directory where backup files are stored.
func processEvent(ev *replication.BinlogEvent, currentFile *os.File, dirPath string) {
	if rotateEv, ok := ev.Event.(*replication.RotateEvent); ok {
		log.Printf("received RotateEvent: switching to new binlog file: %s", string(rotateEv.NextLogName))
		if len(buffer) > 0 {
			writeBufferToFile(currentFile)
		}
		rotateFile(currentFile, dirPath)
		currentBinlog = string(rotateEv.NextLogName)
		return
	}

	raw := ev.RawData
	buffer = append(buffer, raw...)
	StreamBinlogToS3(buffer, currentFile.Name())

	if len(buffer) >= bufferSize {
		writeBufferToFile(currentFile)
	}

	if currentSize >= maxFileSize {
		rotateFile(currentFile, dirPath)
	}

	log.Printf("processed event: %T at pos %d", ev.Event, ev.Header.LogPos)
}

// writeBufferToFile writes the buffered binlog data to the current backup file.
//
// Parameters:
// - currentFile: The current backup file being written to.
func writeBufferToFile(currentFile *os.File) {
	n, err := currentFile.Write(buffer)
	if err != nil {
		log.Printf("failed writing to backup file: %v", err)
		return
	}
	currentSize += int64(n)
	buffer = buffer[:0]
}

// rotateFile closes the current backup file and creates a new one.
//
// Parameters:
// - file: The current backup file to be rotated.
// - dirPath: The directory where the new backup file will be created.
func rotateFile(file *os.File, dirPath string) {
	if len(buffer) > 0 {
		if _, err := file.Write(buffer); err != nil {
			log.Printf("failed flushing remaining data: %v", err)
		}
		buffer = buffer[:0]
	}
	file.Close()
	rotatedFileName := currentFile.Name()

	go func(fileName string) {
		data, err := os.ReadFile(currentFile.Name())
		if err != nil {
			log.Printf("Error reading backup file: %v", err)
			return
		}
		logFile := filepath.Base(fileName)
		UploadBufferToS3(data, logFile)
	}(rotatedFileName)

	var err error
	currentFile, err = openNewFile(dirPath)
	if err != nil {
		log.Fatalf("Cannot create new backup file: %v", err)
	}
	currentSize = 0
}

// getLastBinlogPosition retrieves the last binlog position from the metadata file.
//
// Parameters:
// - metadataFile: The path to the metadata file.
//
// Returns:
// - mysql.Position: The last binlog position.
func getLastBinlogPosition(metadataFile string) mysql.Position {
	file, err := os.Open(metadataFile)
	if err != nil {
		log.Printf("failed to open binlog metadata file: %v", err)
	}
	defer file.Close()

	var binlogFile string
	var binlogPos uint32
	_, err = fmt.Fscanf(file, "%s %d", &binlogFile, &binlogPos)
	if err != nil {
		log.Printf("error reading binlog position: %v", err)
	}

	log.Printf("resuming incremental backup from binlog file: %s at position %d", binlogFile, binlogPos)

	return mysql.Position{Name: binlogFile, Pos: binlogPos}
}

// MysqlIncrementalBackup starts an incremental backup by streaming binlog events.
//
// Parameters:
// - ctx: The context for managing cancellations.
// - backupDir: The directory where backup files will be stored.
//
// Returns:
// - error: An error if the incremental backup process fails, otherwise nil.
func (db *DB) MysqlIncrementalBackup(ctx context.Context, backupDir string) error {
	log.Print("MySQL incremental backup started...")
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     db.Host,
		Port:     uint16(db.Port),
		User:     db.User,
		Password: db.Password,
	}

	metadataFile := fmt.Sprintf("%s/binlog_position.txt", backupDir)

	pos := getLastBinlogPosition(metadataFile)

	syncer := replication.NewBinlogSyncer(cfg)

	streamer, err := syncer.StartSync(pos)
	if err != nil {
		return fmt.Errorf("failed to start binlog sync: %w", err)
	}
	streamData(ctx, streamer, backupDir)
	return nil
}
