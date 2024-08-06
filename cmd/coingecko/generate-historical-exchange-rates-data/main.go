package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/storage"
	api "github.com/0xpetersatoshi/blockchain-data-aggregator/internal/api/coingecko"
	"github.com/0xpetersatoshi/blockchain-data-aggregator/internal/config"
	"github.com/0xpetersatoshi/blockchain-data-aggregator/internal/logger"
	"github.com/0xpetersatoshi/blockchain-data-aggregator/internal/processor"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
)

var (
	sourceBucketName = pflag.String("source-bucket-name", "sequence-blockchain-data-aggregator", "source bucket name")
	sourceObjectPath = pflag.String("source-object-path", "aggregator-data/sample_data.csv", "source object path")
	debug            = pflag.Bool("debug", false, "enable debug logging")
	outputFilepath   = pflag.String("output-filepath", "", "output filepath")
)

func init() {
	// Load the .env file
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file")
	}
	pflag.Parse()
}

func main() {
	logger := logger.GetLogger()
	if !*debug {
		logger = logger.Level(zerolog.InfoLevel)
	}
	ctx := context.Background()
	logger.Info().Msg("starting coingecko client")
	client := api.NewCoinGeckoClient(ctx, logger)
	price, err := client.GetHistoricalTokenPriceUSD("sunflower-land", "01-04-2024")
	if err != nil {
		logger.Fatal().Err(err).Msg("error getting historical token price")
	}
	logger.Info().Msgf("price: %f", price)
	logger.Info().Msgf("source bucket name: %s", *sourceBucketName)
	logger.Info().Msgf("source object path: %s", *sourceObjectPath)
	if *sourceBucketName == "" || *sourceObjectPath == "" || *outputFilepath == "" {
		logger.Fatal().Msg("source bucket name, source object path, or db connection string is empty")
	}

	storageConfig := config.NewStorageConfig(*sourceBucketName, *sourceObjectPath)
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create storage client")
	}

	opts := &processor.Options{SkipExchangeRateConversion: true}
	processor := processor.NewTransactionsProcessor(ctx, storageClient, storageConfig, config.Storage{}, logger, opts)
	m, err := processor.GenerateHistoricalTokenPricesMap()
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to generate historical token price map")
	}
	logger.Info().Interface("map", m).Msg("prices map")

	exchangeRates := make(map[string]map[string]float64)

	for date, tokens := range m {
		for _, token := range tokens {
			flippedDate, err := convertDateString(date)
			if err != nil {
				logger.Fatal().Err(err).Msg("error converting date string")
			}
			tokenID, err := client.GetTokenIDByContractAddress(token.ChainID, token.ContractAddress)
			if err != nil {
				logger.Fatal().Err(err).Msg("error getting token id")
			}
			price, err := client.GetHistoricalTokenPriceUSD(tokenID, flippedDate)
			if err != nil {
				logger.Fatal().
					Err(err).
					Str("date", date).
					Str("symbol", token.Symbol).
					Msg("error getting historical token price")
			}
			logger.Info().Str("date", date).Str("symbol", token.Symbol).Msgf("price: %f", price)

			// Ensure that the inner map is initialized
			if exchangeRates[date] == nil {
				exchangeRates[date] = make(map[string]float64)
			}

			exchangeRates[date][token.Symbol] = price
		}
	}

	jsonData, err := json.MarshalIndent(exchangeRates, "", "  ")
	if err != nil {
		logger.Fatal().Err(err).Msg("error marshalling json")
	}

	err = os.WriteFile(*outputFilepath, jsonData, 0644)
	if err != nil {
		logger.Fatal().Err(err).Msg("error writing json to file")
	}
}

// convertDateString converts a date string from YYYY-MM-DD to DD-MM-YYYY
func convertDateString(dateStr string) (string, error) {
	// Parse the input date string
	parsedDate, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return "", fmt.Errorf("invalid date format: %v", err)
	}

	// Format the parsed date to DD-MM-YYYY
	return parsedDate.Format("02-01-2006"), nil
}
