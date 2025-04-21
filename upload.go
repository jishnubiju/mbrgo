package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// StreamBinlogToS3 streams binary log data to an S3 bucket.
// It writes the provided binary log data to an S3 object using an io.Pipe for streaming.
//
// Parameters:
// - data: The binary log data to be streamed.
// - fileName: The name of the file to be used for generating the S3 key.
//
// Returns:
// - error: An error if the streaming or upload fails, otherwise nil.
func StreamBinlogToS3(data []byte, fileName string) error {
	log.Print("streaming binlog to S3 function started...")

	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		return fmt.Errorf("AWS_S3_BUCKET environment variable is not set")
	}

	key, err := getStreamS3Key(fileName)
	if err != nil {
		return fmt.Errorf("failed to get S3 key for file %s: %w", fileName, err)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return fmt.Errorf("unable to load AWS SDK config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(client)

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		if _, err := pw.Write(data); err != nil {
			log.Printf("failed writing to pipe: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   pr,
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	log.Printf("upload successful: %s", result.Location)
	return nil
}

// UploadBufferToS3 uploads a byte slice to an S3 bucket.
// It uploads the provided data as an S3 object using a buffer.
//
// Parameters:
// - data: The byte slice containing the data to be uploaded.
// - fileName: The name of the file to be used for generating the S3 key.
//
// Returns:
// - error: An error if the upload fails, otherwise nil.
func UploadBufferToS3(data []byte, fileName string) error {
	log.Print("upload buffer to S3 function started...")

	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		return fmt.Errorf("AWS_S3_BUCKET environment variable is not set")
	}

	key, err := getS3Key(fileName)
	if err != nil {
		return fmt.Errorf("failed to get S3 key for file %s: %w", fileName, err)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return fmt.Errorf("unable to load AWS SDK config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(client)

	buf := bytes.NewReader(data)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   buf,
	})
	if err != nil {
		return fmt.Errorf("failed to upload buffer to S3: %w", err)
	}

	log.Printf("Upload successful: %s", result.Location)
	return nil
}

// getS3Key generates the S3 key for a given file name.
// It determines the S3 key based on the type of backup (full or incremental).
//
// Parameters:
// - fileName: The name of the file for which the S3 key is to be generated.
//
// Returns:
// - string: The generated S3 key.
// - error: An error if the file name is invalid or the date parsing fails.
func getS3Key(fileName string) (string, error) {
	if strings.Contains(fileName, "full_backup") {
		tokens := strings.SplitN(fileName, "_", 2)
		if len(tokens) < 1 {
			return "", fmt.Errorf("invalid full backup file name: %s", fileName)
		}
		dateStr := tokens[0]
		t, err := time.Parse("20060102", dateStr)
		if err != nil {
			return "", fmt.Errorf("failed to parse date %s: %w", dateStr, err)
		}
		year, week := t.ISOWeek()
		return fmt.Sprintf("%d/%02d/%s", year, week, fileName), nil
	}

	if strings.Contains(fileName, "incr_backup") {
		tokens := strings.Split(fileName, "_")
		if len(tokens) < 5 {
			return "", fmt.Errorf("invalid incremental backup file name: %s", fileName)
		}
		dateStr := tokens[len(tokens)-2]
		t, err := time.Parse("20060102", dateStr)
		if err != nil {
			return "", fmt.Errorf("failed to parse date %s: %w", dateStr, err)
		}
		year, week := t.ISOWeek()
		weekday := t.Weekday().String()
		return fmt.Sprintf("%d/%02d/%s/%s", year, week, weekday, fileName), nil
	}

	return "", fmt.Errorf("unknown backup file type: %s", fileName)
}

// getStreamS3Key generates the S3 key for streaming data.
// It determines the S3 key based on the type of backup (incremental).
//
// Parameters:
// - fileName: The name of the file for which the S3 key is to be generated.
//
// Returns:
// - string: The generated S3 key.
// - error: An error if the file name is invalid or the date parsing fails.
func getStreamS3Key(fileName string) (string, error) {
	if strings.Contains(fileName, "incr_backup") {
		tokens := strings.Split(fileName, "_")
		if len(tokens) < 5 {
			return "", fmt.Errorf("invalid incremental backup file name: %s", fileName)
		}
		dateStr := tokens[len(tokens)-2]
		t, err := time.Parse("20060102", dateStr)
		if err != nil {
			return "", fmt.Errorf("failed to parse date %s: %w", dateStr, err)
		}
		year, week := t.ISOWeek()
		return fmt.Sprintf("%d/%02d/%s", year, week, "weekly-binlog.log"), nil
	}

	return "", fmt.Errorf("unknown backup file type: %s", fileName)
}
