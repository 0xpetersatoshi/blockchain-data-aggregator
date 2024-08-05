package processor

// Record defines the interface for a single record
type Record interface {
	DBRowEntry() []interface{}
	Values() string
}

// RecordBatcher defines the interface for a struct containing records to be inserted into the database
type RecordBatcher interface {
	TableName() string
	NumColumns() int
	Columns() []string
	Records() []Record
}

// Processor defines a generic ETL processor
type Processor interface {
	Process() (RecordBatcher, error)
}
