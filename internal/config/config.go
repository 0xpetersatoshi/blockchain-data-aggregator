package config

// Storage defines the GCS configuration details
type Storage struct {
	BucketName, ObjectPath string
}

// NewStorageConfig creates a new StorageConfig
func NewStorageConfig(bucketName, objectPath string) Storage {
	return Storage{
		BucketName: bucketName,
		ObjectPath: objectPath,
	}
}
