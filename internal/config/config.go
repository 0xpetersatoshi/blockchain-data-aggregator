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

// Processor defines the configuration for the ETL processor
type Processor struct {
	BatchSize int
}

// NewProcessorConfig creates a new Config for an ETL processor
func NewProcessorConfig(batchSize int) Processor {
	return Processor{
		BatchSize: batchSize,
	}
}
