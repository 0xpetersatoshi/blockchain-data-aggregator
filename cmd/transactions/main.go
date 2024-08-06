package main

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/0xpetersatoshi/blockchain-data-aggregator/internal/config"
	"github.com/0xpetersatoshi/blockchain-data-aggregator/internal/loader"
	"github.com/0xpetersatoshi/blockchain-data-aggregator/internal/logger"
	"github.com/0xpetersatoshi/blockchain-data-aggregator/internal/processor"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
)

var (
	sourceBucketName = pflag.String("source-bucket-name", "sequence-blockchain-data-aggregator", "source bucket name")
	sourceObjectPath = pflag.String("source-object-path", "aggregator-data/sample_data.csv", "source object path")
	debug            = pflag.Bool("debug", false, "enable debug logging")
	dbConnString     = pflag.String("db-conn-string", "clickhouse://default:password@localhost:9000/default", "database connection string")
)

func init() {
	pflag.Parse()
}

func main() {
	logger := logger.GetLogger()

	if !*debug {
		logger = logger.Level(zerolog.InfoLevel)
	}
	logger.Info().Msg("processing transactions data")

	logger.Info().Msgf("source bucket name: %s", *sourceBucketName)
	logger.Info().Msgf("source object path: %s", *sourceObjectPath)
	if *sourceBucketName == "" || *sourceObjectPath == "" || *dbConnString == "" {
		logger.Fatal().Msg("source bucket name, source object path, or db connection string is empty")
	}

	ctx := context.Background()
	storageConfig := config.NewStorageConfig(*sourceBucketName, *sourceObjectPath)
	processorConfig := config.NewProcessorConfig(100)
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create storage client")
	}
	processor := processor.NewTransactionsProcessor(ctx, processorConfig, storageClient, storageConfig, logger)

	// Start processing
	batch, err := processor.Process()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to process transactions data")
	}

	for _, record := range batch.Records() {
		logger.Info().Msg("record: " + record.Values())
	}

	logger.Info().Msg("loading transactions data into clickhouse")
	logger.Debug().Msgf("db connection string: %s", *dbConnString)
	clickhouseLoader := loader.NewClickHouseLoader(*dbConnString, logger)
	if err := clickhouseLoader.Connect(ctx); err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to clickhouse")
	}
	if err := clickhouseLoader.Load(ctx, batch); err != nil {
		logger.Fatal().Err(err).Msg("failed to load transactions data")
	}

	if err := clickhouseLoader.Close(ctx); err != nil {
		logger.Fatal().Err(err).Msg("failed to close clickhouse loader")
	}

	logger.Info().Msg("finished processing transactions data")
}
