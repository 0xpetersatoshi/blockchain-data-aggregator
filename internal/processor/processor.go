package processor

// Record defines the interface for a record to be inserted into the database
type Record interface {
	TableName() string
	DBRowEntry() []interface{}
}

// Processor defines a generic ETL processor
type Processor interface {
	Process() error
}
