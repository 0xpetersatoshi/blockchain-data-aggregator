package processor

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"
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
	numRecordFields := reflect.TypeOf(*newRecord).NumField()
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

// TransactionRecord defines the individual record for the transactions data
type TransactionRecord struct {
	Date           string
	ProjectID      string
	CurrencySymbol string
	CurrencyValue  float64
}

// AggregatedTransactionRecord defines the aggregated record for the transactions data
type AggregatedTransactionRecord struct {
	Date                 string  `json:"date"`
	ProjectID            string  `json:"project_id"`
	NumberOfTransactions int     `json:"number_of_transactions"`
	TotalVolumeUSD       float64 `json:"total_volume_usd"`
}

// TableName returns the table name
func (t *AggregatedTransactionRecord) TableName() string {
	return "transactions"
}

// DBRowEntry returns the database row entry
func (t *AggregatedTransactionRecord) DBRowEntry() []interface{} {
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
	doneChan      chan<- bool
	config        config.Processor
	storageClient *storage.Client
	storageConfig config.Storage
	logger        zerolog.Logger
}

// NewTransactionsProcessor creates a new TransactionsProcessor
func NewTransactionsProcessor(ctx context.Context, recordChan chan<- Record, doneChan chan<- bool, config config.Processor, storageClient *storage.Client, storageConfig config.Storage, logger zerolog.Logger) *TransactionsProcessor {
	return &TransactionsProcessor{
		context:       ctx,
		recordChan:    recordChan,
		doneChan:      doneChan,
		config:        config,
		storageClient: storageClient,
		storageConfig: storageConfig,
		logger:        logger,
	}
}

// Process processes the transactions data
func (t *TransactionsProcessor) Process() error {
	if err := t.processCSV(); err != nil {
		return err
	}
	return nil
}

func (t *TransactionsProcessor) processRawRecord(record *RawTransactionRecord) (*TransactionRecord, error) {
	// TODO: flatten and parse data into TransactionRecord
	// TODO: convert token prices using coin gecko api data

	const layout = "2006-01-02 15:04:05.000"

	transactionRecord := &TransactionRecord{}

	ts, err := time.Parse(layout, record.Timestamp)
	if err != nil {
		return nil, err
	}

	transactionRecord.Date = ts.Format("2006-01-02")
	transactionRecord.ProjectID = record.ProjectID

	return transactionRecord, nil
}

func (t *TransactionsProcessor) processCSV() error {
	reader, err := utils.GetGCSReader(t.context, t.storageClient, t.storageConfig.BucketName, t.storageConfig.ObjectPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	csvReader := csv.NewReader(reader)

	// Skip header
	header, err := csvReader.Read()
	if err != nil {
		return err
	}

	t.logger.Debug().Msgf("Header: %+v", header)

	var processedRecords []*TransactionRecord
	var count int
	defer close(t.doneChan)
	// TODO: implement batch processing
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			t.logger.Debug().Msgf("Processed %d records", count)
			break
		}
		if err != nil {
			return err
		}
		t.logger.Debug().Msgf("Record: %+v", record)

		newRawRecord, err := NewRawTransactionRecord(record)
		if err != nil {
			return err
		}

		newTransactionRecord, err := t.processRawRecord(newRawRecord)
		if err != nil {
			return err
		}

		processedRecords = append(processedRecords, newTransactionRecord)

		t.logger.Debug().Int("count", count).Msgf("TransactionRecord: %+v", newTransactionRecord)

		if count%100 == 0 {
			t.logger.Info().Msgf("processed %d records", count)
		}

		count++
	}

	var wg sync.WaitGroup
	aggregated := aggregateRecords(processedRecords)
	for key, record := range aggregated {
		wg.Add(1)
		go func(key string, record *AggregatedTransactionRecord) {
			defer wg.Done()
			for {
				select {
				case t.recordChan <- record:
					bs, err := json.MarshalIndent(record, "", "  ")
					if err != nil {
						t.logger.Error().Err(err).Msg("Error marshalling record")
					} else {
						t.logger.Debug().Str("key", key).Msgf("Record: %s", string(bs))
					}
					return
				default:
					t.logger.Warn().Msgf("Record chan is full")
					time.Sleep(100 * time.Millisecond)
				}
			}
		}(key, record)
	}
	wg.Wait()
	t.doneChan <- true
	return nil
}

func aggregateRecords(records []*TransactionRecord) map[string]*AggregatedTransactionRecord {
	aggregated := make(map[string]*AggregatedTransactionRecord)
	for _, record := range records {
		key := fmt.Sprintf("%s-%s", record.Date, record.ProjectID)
		if _, ok := aggregated[key]; !ok {
			aggregated[key] = &AggregatedTransactionRecord{}
			aggregated[key].Date = record.Date
			aggregated[key].ProjectID = record.ProjectID
		}
		aggregated[key].NumberOfTransactions++
		// TODO: convert token prices to USD
		aggregated[key].TotalVolumeUSD += record.CurrencyValue
	}
	return aggregated
}
