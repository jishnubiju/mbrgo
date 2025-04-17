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

func s3Download(backupS3Dir string, restorePath string) error {
	log.Print("s3 download function started..!")

	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		return fmt.Errorf("AWS_S3_BUCKET environment variable is not set")
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return fmt.Errorf("unable to load AWS SDK config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	downloader := manager.NewDownloader(client)

	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(backupS3Dir),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	listOutput, err := client.ListObjectsV2(ctx, listInput)
	if err != nil {
		return fmt.Errorf("failed to list objects in backup S3 directory %s: %w", backupS3Dir, err)
	}

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

func downloadFile(ctx context.Context, downloader *manager.Downloader, bucket, key, destFile string) error {
	currentFile, err := os.Create(destFile)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", destFile, err)
	}
	defer currentFile.Close()

	_, err = downloader.Download(ctx, currentFile, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download file %s: %w", key, err)
	}

	return nil
}
