package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/rs/zerolog"
)

type CoinGeckoClient struct {
	ctx     context.Context
	baseURL string
	logger  zerolog.Logger
	apiKey  string
}

func NewCoinGeckoClient(ctx context.Context, logger zerolog.Logger) *CoinGeckoClient {
	return &CoinGeckoClient{
		ctx:     ctx,
		baseURL: "https://api.coingecko.com/api/v3",
		logger:  logger,
		apiKey:  os.Getenv("API_KEY"),
	}
}

func (c *CoinGeckoClient) doRequest(endpoint string) ([]byte, error) {
	url := fmt.Sprintf("%s/%s", c.baseURL, endpoint)
	c.logger.Debug().Str("url", url).Msg("Making request")

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("accept", "application/json")
	req.Header.Add("x-cg-api-key", c.apiKey)

	// Retry mechanism with exponential backoff
	const maxRetries = 5
	var retries int

	for {
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		if res.StatusCode == http.StatusOK {
			return io.ReadAll(res.Body)
		}

		if res.StatusCode == http.StatusTooManyRequests {
			if retries >= maxRetries {
				body, _ := io.ReadAll(res.Body)
				return nil, fmt.Errorf("API request failed after %d retries with status %d: %s", retries, res.StatusCode, string(body))
			}
			retries++
			backoffDuration := time.Duration(math.Pow(2, float64(retries))) * time.Second
			c.logger.Warn().Int("retry", retries).Dur("backoff", backoffDuration).Msg("Rate limited, retrying with backoff")
			time.Sleep(backoffDuration)
			continue
		}

		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", res.StatusCode, string(body))
	}
}

func (c *CoinGeckoClient) GetTokenPrice(tokenContractAddr string, networkID string) (float64, error) {
	endpoint := fmt.Sprintf("onchain/networks/%s/tokens/%s", networkID, tokenContractAddr)
	body, err := c.doRequest(endpoint)
	if err != nil {
		return 0, err
	}

	var result struct {
		Price float64 `json:"price"`
	}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return 0, err
	}

	return result.Price, nil
}

// GetHistoricalTokenPriceUSD gets the USD price of a token on a given date given the coin gecko token id and a datestring in the DD-MM-YYYY format
func (c *CoinGeckoClient) GetHistoricalTokenPriceUSD(cGTokenID, dateString string) (float64, error) {
	c.logger.Debug().Msgf("getting historical token price for %s on %s", cGTokenID, dateString)
	endpoint := fmt.Sprintf("coins/%s/history?date=%s", cGTokenID, dateString)
	body, err := c.doRequest(endpoint)
	if err != nil {
		return 0, err
	}

	var response HistoricalTokenDataResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return 0, err
	}
	return response.MarketData.CurrentPrice.Usd, nil
}

func (c *CoinGeckoClient) GetCoinList() (interface{}, error) {
	endpoint := "/coins/list"
	body, err := c.doRequest(endpoint)
	if err != nil {
		return nil, err
	}

	var response CoinListResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	return body, nil
}

// GetTokenDataByContractAddress gets the token data by contract address
func (c *CoinGeckoClient) GetTokenIDByContractAddress(chainID string, tokenContractAddr string) (string, error) {
	var platformID string
	switch chainID {
	case "137":
		platformID = "polygon_pos"
	default:
		return "", fmt.Errorf("unmapped chain id: %s", chainID)
	}

	// Special logic for handling USDC
	if chainID == "137" && tokenContractAddr == "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359" {
		return "usd-coin", nil
	}

	// Special handling for matic
	if chainID == "137" && tokenContractAddr == "0x0000000000000000000000000000000000000000" {
		return "matic-network", nil
	}

	endpoint := fmt.Sprintf("coins/%s/contract/%s", platformID, tokenContractAddr)
	body, err := c.doRequest(endpoint)
	if err != nil {
		return "", err
	}

	var response CoinDataTokenAddressResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", err
	}
	return response.ID, nil
}
