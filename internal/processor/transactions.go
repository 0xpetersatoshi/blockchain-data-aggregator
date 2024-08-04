package processor

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"time"

	"cloud.google.com/go/storage"
	"github.com/0xpetersatoshi/blockchain-data-aggregator/internal/config"
	"github.com/0xpetersatoshi/blockchain-data-aggregator/internal/utils"
	"github.com/rs/zerolog"
)

// TransactionProps defines the properties of the transaction
type TransactionProps struct {
	TokenID           string `json:"tokenId"`
	TxnHash           string `json:"txnHash"`
	ChainID           string `json:"chainId"`
	CollectionAddress string `json:"collectionAddress"`
	CurrencyAddress   string `json:"currencyAddress"`
	CurrencySymbol    string `json:"currencySymbol"`
	MarketplaceType   string `json:"marketplaceType"`
	RequestID         string `json:"requestId"`
}

// TransactionNums defines financial numbers of the transaction
type TransactionNums struct {
	CurrencyValueDecimal string `json:"currencyValueDecimal"`
	CurrencyValueRaw     string `json:"currencyValueRaw"`
}

// RawTransactionRecord defines the raw record for the transactions data
type RawTransactionRecord struct {
	App                  string           `csv:"app"`
	Timestamp            string           `csv:"ts"`
	Event                string           `csv:"event"`
	ProjectID            string           `csv:"project_id"`
	Source               string           `csv:"source"`
	Ident                string           `csv:"ident"`
	UserID               string           `csv:"user_id"`
	SessionID            string           `csv:"session_id"`
	Country              string           `csv:"country"`
	DeviceType           string           `csv:"device_type"`
	DeviceOS             string           `csv:"device_os"`
	DeviceOSVersion      string           `csv:"device_os_ver"`
	DeviceBrowser        string           `csv:"device_browser"`
	DeviceBrowserVersion string           `csv:"device_browser_ver"`
	Props                TransactionProps `csv:"props"`
	Nums                 TransactionNums  `csv:"nums"`
}

// NewRawTransactionRecord creates a new raw transaction struct
func NewRawTransactionRecord(record []string) (*RawTransactionRecord, error) {
	newRecord := &RawTransactionRecord{}
	numRecordFields := reflect.TypeOf(newRecord).NumField()
	numRawRecordFields := len(record)

	if numRawRecordFields < numRecordFields {
		return nil, fmt.Errorf("csv record does not have enough fields to create a RawTransactionRecord. Want %d but got %d", numRecordFields, numRawRecordFields)
	}

	newRecord.App = record[0]
	newRecord.Timestamp = record[1]
	newRecord.Event = record[2]
	newRecord.ProjectID = record[3]
	newRecord.Source = record[4]
	newRecord.Ident = record[5]
	newRecord.UserID = record[6]
	newRecord.SessionID = record[7]
	newRecord.Country = record[8]
	newRecord.DeviceType = record[9]
	newRecord.DeviceOS = record[10]
	newRecord.DeviceOSVersion = record[11]
	newRecord.DeviceBrowser = record[12]
	newRecord.DeviceBrowserVersion = record[13]

	// Unmarshal props and nums
	if err := json.Unmarshal([]byte(record[14]), &newRecord.Props); err != nil {
		return nil, err
	}

	if err := json.Unmarshal([]byte(record[15]), &newRecord.Nums); err != nil {
		return nil, err
	}

	return newRecord, nil
}

// TransactionRecord defines the record for the transactions data
type TransactionRecord struct {
	Date                 time.Time
	ProjectID            string
	NumberOfTransactions int
	TotalVolumeUSD       float64
}

// NewTransactionRecord creates a new TransactionRecord
func NewTransactionRecord(date time.Time, projectID string, numberOfTransactions int, totalVolumeUSD float64) *TransactionRecord {
	return &TransactionRecord{
		Date:                 date,
		ProjectID:            projectID,
		NumberOfTransactions: numberOfTransactions,
		TotalVolumeUSD:       totalVolumeUSD,
	}
}

// TableName returns the table name
func (t *TransactionRecord) TableName() string {
	return "transactions"
}

// DBRowEntry returns the database row entry
func (t *TransactionRecord) DBRowEntry() []interface{} {
	return []interface{}{
		t.Date,
		t.ProjectID,
		t.NumberOfTransactions,
		t.TotalVolumeUSD,
	}
}

// TransactionsProcessor defines the ETL processor for the transactions data
type TransactionsProcessor struct {
	context       context.Context
	recordChan    chan<- Record
	config        config.Processor
	storageClient *storage.Client
	storageConfig config.Storage
	logger        zerolog.Logger
}

// NewTransactionsProcessor creates a new TransactionProcessor
func NewTransactionsProcessor(ctx context.Context, recordChan chan<- Record, config config.Processor, storageClient *storage.Client, storageConfig config.Storage, logger zerolog.Logger) *TransactionsProcessor {
	return &TransactionsProcessor{
		context:       ctx,
		recordChan:    recordChan,
		config:        config,
		storageClient: storageClient,
		storageConfig: storageConfig,
		logger:        logger,
	}
}

// Process processes the transactions data
func (t *TransactionsProcessor) Process() error {
	return nil
}

func (t *TransactionsProcessor) processRawRecord(record *RawTransactionRecord) (*TransactionRecord, error) {
	// TODO: flatten and parse data into TransactionRecord
	// TODO: convert token prices using coin gecko api data
	return nil, nil
}

func (t *TransactionsProcessor) processCSV() error {
	reader, err := utils.GetGCSReader(t.context, t.storageClient, t.storageConfig.BucketName, t.storageConfig.ObjectPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	csvReader := csv.NewReader(reader)

	// Skip header
	if _, err := csvReader.Read(); err != nil {
		return err
	}

	// TODO: implement batch processing
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		newRawRecord, err := NewRawTransactionRecord(record)
		if err != nil {
			return err
		}

		newTransactionRecord, err := t.processRawRecord(newRawRecord)

		// Put into channel
		t.recordChan <- newTransactionRecord
	}
	return nil
}
