package utils

import (
	"context"

	"cloud.google.com/go/storage"
)

// GetGCSReader returns a GCS reader
func GetGCSReader(ctx context.Context, client *storage.Client, bucketName, path string) (*storage.Reader, error) {
	obj := client.Bucket(bucketName).Object(path)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return reader, nil
}
