package main

import "github.com/0xpetersatoshi/blockchain-data-aggregator/internal/logger"

func main() {
	logger := logger.GetLogger()
	logger.Info().Msg("Hello World!")
}
