package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3Download downloads all files from a specified S3 directory to a local restore path.
//
// Parameters:
// - backupS3Dir: The S3 directory (prefix) containing the backup files to download.
// - restorePath: The local directory where the downloaded files will be stored.
//
// Returns:
// - error: An error if the download process fails, otherwise nil.
func s3Download(backupS3Dir string, restorePath string) error {
	log.Print("s3 download function started..!")

	// Retrieve the S3 bucket name from the environment variable.
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		return fmt.Errorf("AWS_S3_BUCKET environment variable is not set")
	}

	// Load the AWS SDK configuration.
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return fmt.Errorf("unable to load AWS SDK config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	downloader := manager.NewDownloader(client)

	// Prepare the input for listing objects in the specified S3 directory.
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(backupS3Dir),
	}

	// Set a timeout for the S3 operations.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// List all objects in the specified S3 directory.
	listOutput, err := client.ListObjectsV2(ctx, listInput)
	if err != nil {
		return fmt.Errorf("failed to list objects in backup S3 directory %s: %w", backupS3Dir, err)
	}

	// Iterate through the listed objects and download each file.
	for _, object := range listOutput.Contents {
		key := *object.Key
		destFile := filepath.Join(restorePath, filepath.Base(key))
		log.Printf("Downloading %s to %s", key, destFile)

		if err := downloadFile(ctx, downloader, bucket, key, destFile); err != nil {
			log.Printf("failed to download file %s: %v", key, err)
		} else {
			log.Printf("download successful for file %s", key)
		}
	}
	return nil
}

// downloadFile downloads a single file from an S3 bucket to a local file.
//
// Parameters:
// - ctx: The context for managing timeouts and cancellations.
// - downloader: The S3 downloader instance.
// - bucket: The name of the S3 bucket.
// - key: The S3 key (file path) of the file to download.
// - destFile: The local file path where the downloaded file will be saved.
//
// Returns:
// - error: An error if the download process fails, otherwise nil.
func downloadFile(ctx context.Context, downloader *manager.Downloader, bucket, key, destFile string) error {
	// Create the local file where the downloaded content will be stored.
	currentFile, err := os.Create(destFile)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", destFile, err)
	}
	defer currentFile.Close()

	// Download the file from S3 to the local file.
	_, err = downloader.Download(ctx, currentFile, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download file %s: %w", key, err)
	}

	return nil
}
