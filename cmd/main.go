package main

import (
	"context"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/0xpetersatoshi/blockchain-data-aggregator/internal/config"
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
	if *sourceBucketName == "" || *sourceObjectPath == "" {
		logger.Fatal().Msg("source bucket name or source object path is empty")
	}

	ctx := context.Background()
	recordChan := make(chan processor.Record, 100)
	doneChan := make(chan bool)
	storageConfig := config.NewStorageConfig(*sourceBucketName, *sourceObjectPath)
	processorConfig := config.NewProcessorConfig(100)
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create storage client")
	}
	processor := processor.NewTransactionsProcessor(ctx, recordChan, doneChan, processorConfig, storageClient, storageConfig, logger)

	var wg sync.WaitGroup

	// Start a goroutine to read from the channel
	wg.Add(1)
	go func(log zerolog.Logger) {
		defer wg.Done()
		for {
			select {
			case <-doneChan:
				return
			case record, ok := <-recordChan:
				if !ok {
					return
				}
				loadRecord(record, log)
			}
		}
	}(logger)

	// Start processing
	if err := processor.Process(); err != nil {
		log.Fatal().Err(err).Msg("failed to process transactions data")
	}

	// Close the record channel after processing is done
	close(recordChan)
	wg.Wait() // Wait for all goroutines to finish
	logger.Info().Msg("finished processing transactions data")
}

// loadRecord loads a transaction record into the database
func loadRecord(record processor.Record, logger zerolog.Logger) {
	// TODO: implement loading logic
	logger.Info().Msgf("Processed record: %+v\n", record)
}
