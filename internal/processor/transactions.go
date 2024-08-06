package processor

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"reflect"
	"strconv"
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

type Token struct {
	Symbol          string
	ChainID         string
	ContractAddress string
}

// TransactionRecord defines the individual record for the transactions data
type TransactionRecord struct {
	Date             string
	ProjectID        string
	CurrencyValueUSD float64
	Token
}

// AggregatedTransactionRecord defines the aggregated record for the transactions data
type AggregatedTransactionRecord struct {
	Date                 string  `json:"date" sql:"date"`
	ProjectID            string  `json:"project_id" sql:"project_id"`
	NumberOfTransactions int     `json:"number_of_transactions" sql:"number_of_transactions"`
	TotalVolumeUSD       float64 `json:"total_volume_usd" sql:"total_volume_usd"`
}

// DBRowEntry returns the database row entry
func (t *AggregatedTransactionRecord) DBRowEntry() []interface{} {
	return utils.GetStructFields(t)
}

// Values returns the values
func (t *AggregatedTransactionRecord) Values() string {
	bs, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return ""
	}
	return string(bs)
}

// TransactionsBatch defines the record batch for the aggregated transactions data
type TransactionsBatch struct {
	records []*AggregatedTransactionRecord
}

// NewTransactionsBatch creates a new TransactionsTransporter
func NewTransactionsBatch(records []*AggregatedTransactionRecord) *TransactionsBatch {
	return &TransactionsBatch{
		records: records,
	}
}

// TableName returns the table name
func (t *TransactionsBatch) TableName() string {
	return "transactions"
}

// NumColumns returns the number of columns
func (t *TransactionsBatch) NumColumns() int {
	return reflect.TypeOf(*t).NumField()
}

// Columns returns the column names of the transactions record
func (t *TransactionsBatch) Columns() []string {
	s := &AggregatedTransactionRecord{}
	return utils.GetStructFieldNames(s, "sql")
}

// Records returns the records
func (t *TransactionsBatch) Records() []Record {
	var records []Record
	for _, r := range t.records {
		records = append(records, r)
	}
	return records
}

type Options struct {
	SkipExchangeRateConversion bool
}

// TransactionsProcessor defines the ETL processor for the transactions data
type TransactionsProcessor struct {
	context                        context.Context
	storageClient                  *storage.Client
	TransactionsDataStorageConfig  config.Storage
	exchangeRatesDataStorageConfig config.Storage
	logger                         zerolog.Logger
	exchangeRatesData              map[string]map[string]float64
	options                        *Options
}

// NewTransactionsProcessor creates a new TransactionsProcessor
func NewTransactionsProcessor(ctx context.Context, storageClient *storage.Client, storageConfig config.Storage, exchangeRatesStorageConfig config.Storage, logger zerolog.Logger, options *Options) *TransactionsProcessor {
	if options == nil {
		options = &Options{}
	}
	return &TransactionsProcessor{
		context:                        ctx,
		storageClient:                  storageClient,
		TransactionsDataStorageConfig:  storageConfig,
		exchangeRatesDataStorageConfig: exchangeRatesStorageConfig,
		exchangeRatesData:              make(map[string]map[string]float64),
		logger:                         logger,
		options:                        options,
	}
}

// Process processes the transactions data
func (t *TransactionsProcessor) Process() (RecordBatcher, error) {
	if t.options != nil && t.options.SkipExchangeRateConversion {
		t.logger.Info().Msg("Skipping exchange rate conversion")
	} else {
		if err := t.getExchangeRatesData(); err != nil {
			return nil, err
		}
	}
	records, err := t.processCSV()
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (t *TransactionsProcessor) getExchangeRatesData() error {
	reader, err := utils.GetGCSReader(t.context, t.storageClient, t.exchangeRatesDataStorageConfig.BucketName, t.exchangeRatesDataStorageConfig.ObjectPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Read the file contents
	bytes, err := io.ReadAll(reader)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	// Unmarshal the JSON data into the map
	err = json.Unmarshal(bytes, &t.exchangeRatesData)
	if err != nil {
		log.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	return nil
}

func (t *TransactionsProcessor) processRawRecord(record *RawTransactionRecord) (*TransactionRecord, error) {
	const layout = "2006-01-02 15:04:05.000"

	transactionRecord := &TransactionRecord{}

	ts, err := time.Parse(layout, record.Timestamp)
	if err != nil {
		return nil, err
	}

	formattedDate := ts.Format("2006-01-02")

	currencyValueDecimal, err := strconv.ParseFloat(record.Nums.CurrencyValueDecimal, 64)
	if err != nil {
		return nil, err
	}

	var USDValue float64
	if t.options != nil && !t.options.SkipExchangeRateConversion {
		exchangeRate, found := t.exchangeRatesData[formattedDate][record.Props.CurrencySymbol]
		if !found {
			// TODO: fetch/update exchange rates when existing data is missing
			return nil, fmt.Errorf("exchange rate not found for date '%s' and currency '%s'", formattedDate, record.Props.CurrencySymbol)
		}
		USDValue = currencyValueDecimal * exchangeRate
	}

	transactionRecord.Date = formattedDate
	transactionRecord.ProjectID = record.ProjectID
	transactionRecord.Symbol = record.Props.CurrencySymbol
	transactionRecord.CurrencyValueUSD = USDValue
	transactionRecord.ChainID = record.Props.ChainID
	transactionRecord.ContractAddress = record.Props.CurrencyAddress

	return transactionRecord, nil
}

func (t *TransactionsProcessor) getTransactionRecords() ([]*TransactionRecord, error) {
	reader, err := utils.GetGCSReader(t.context, t.storageClient, t.TransactionsDataStorageConfig.BucketName, t.TransactionsDataStorageConfig.ObjectPath)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	csvReader := csv.NewReader(reader)

	// Skip header
	header, err := csvReader.Read()
	if err != nil {
		return nil, err
	}

	t.logger.Debug().Msgf("Header: %+v", header)

	var processedRecords []*TransactionRecord
	var count int
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			t.logger.Debug().Msgf("Processed %d records", count)
			break
		}
		if err != nil {
			return nil, err
		}
		t.logger.Debug().Msgf("Record: %+v", record)

		newRawRecord, err := NewRawTransactionRecord(record)
		if err != nil {
			return nil, err
		}

		newTransactionRecord, err := t.processRawRecord(newRawRecord)
		if err != nil {
			return nil, err
		}

		processedRecords = append(processedRecords, newTransactionRecord)

		t.logger.Debug().Int("count", count).Msgf("TransactionRecord: %+v", newTransactionRecord)

		if count%100 == 0 {
			t.logger.Info().Msgf("processed %d records", count)
		}

		count++
	}

	return processedRecords, nil
}

func (t *TransactionsProcessor) processCSV() (RecordBatcher, error) {
	processedRecords, err := t.getTransactionRecords()
	if err != nil {
		return nil, err
	}
	aggregated := aggregateRecords(processedRecords)
	var records []*AggregatedTransactionRecord
	for _, record := range aggregated {
		records = append(records, record)
	}

	return NewTransactionsBatch(records), nil
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
		aggregated[key].TotalVolumeUSD += record.CurrencyValueUSD
	}
	return aggregated
}

func (t *TransactionsProcessor) GenerateHistoricalTokenPricesMap() (map[string][]Token, error) {
	records, err := t.getTransactionRecords()
	if err != nil {
		return nil, err
	}
	return generateUniqueCurrencyMap(records), nil
}

func generateUniqueCurrencyMap(records []*TransactionRecord) map[string][]Token {
	// Map to track unique currency symbols for each date
	dateCurrencyMap := make(map[string]map[string]Token)

	for _, record := range records {
		if _, ok := dateCurrencyMap[record.Date]; !ok {
			dateCurrencyMap[record.Date] = make(map[string]Token)
		}
		dateCurrencyMap[record.Date][record.Symbol] = Token{Symbol: record.Symbol, ChainID: record.ChainID, ContractAddress: record.ContractAddress}
	}

	// Convert the map of sets to a map of slices
	result := make(map[string][]Token)
	for date, currencySet := range dateCurrencyMap {
		for _, token := range currencySet {
			result[date] = append(result[date], token)
		}
	}

	return result
}
