package ecom

import (
	"context"
	"encoding/json"
	"main/internal/types"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

const TIME_FORMAT = "2006-01-02T15:04:05"

const connectionLimit = 3

type EcomProductInventory struct {
	IsId      uint           `json:"storeId"`
	GoodsId   uint           `json:"goodsId"`
	ValidDate string         `json:"validDate"`
	Quantity  types.Quantity `json:"quantity"`
	EcomPrice types.Price    `json:"ecomPrice"`
}

type ISChangeDate struct {
	PartStocksDate *time.Time
	FullStocksDate *time.Time
}

type EcomConfig struct {
	User     string
	Password string
	Host     string
}

type EcomClient struct {
	config  EcomConfig
	ctx     *context.Context
	limiter *rate.Limiter
}

func (client *EcomClient) newRequest(url string) (*http.Request, error) {
	req, err := http.NewRequest("GET", client.config.Host+url, nil)

	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(client.config.User, client.config.Password)
	req.Header.Add("Accept-Encoding", "identity")

	return req, nil
}

func (client *EcomClient) GetInventoryChangeDate() (map[uint]ISChangeDate, error) {
	req, err := client.newRequest("stocks/stores/")
	if err != nil {
		return nil, err
	}

	httpClient := http.Client{
		Timeout: 20 * time.Second,
	}

	client.limiter.Wait(*client.ctx)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)

	var respJSON struct {
		StoresList []struct {
			StoreId        string `json:"storeId"`
			FullStocksDate string `json:"fullStocksDate"`
			PartStocksDate string `json:"partStocksDate"`
		} `json:"storesList"`
	}

	err = decoder.Decode(&respJSON)
	if err != nil {
		return nil, err
	}

	var result = make(map[uint]ISChangeDate, 0)

	for _, item := range respJSON.StoresList {
		isId, err := strconv.ParseUint(item.StoreId, 10, 32)
		if err != nil {
			continue
		}

		fullStocksDate, _ := time.Parse(TIME_FORMAT, item.FullStocksDate)
		if err != nil {
			continue
		}

		partStocksDate, _ := time.Parse(TIME_FORMAT, item.PartStocksDate)
		if err != nil {
			continue
		}

		result[uint(isId)] = ISChangeDate{
			PartStocksDate: &partStocksDate,
			FullStocksDate: &fullStocksDate,
		}
	}

	return result, nil
}

func (client *EcomClient) GetStocks(isId uint, dateFrom time.Time) (map[uint]EcomProductInventory, error) {

	var query = "?getSeparatedStocks=true"
	if !dateFrom.IsZero() {
		query += "&dateFrom=" + dateFrom.Format(TIME_FORMAT)
	}

	req, err := client.newRequest("stocks/" + strconv.FormatUint(uint64(isId), 10) + query)
	if err != nil {
		return nil, err
	}

	httpClient := http.Client{
		Timeout: 60 * time.Second,
	}

	client.limiter.Wait(*client.ctx)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)

	var inventories struct {
		Stocks []EcomProductInventory `json:"stocks"`
	}

	err = decoder.Decode(&inventories)
	if err != nil {
		return nil, err
	}

	var result = make(map[uint]EcomProductInventory, len(inventories.Stocks))

	for _, item := range inventories.Stocks {
		if curVal, ok := result[item.GoodsId]; ok {
			item.Quantity.Add(curVal.Quantity)
		}

		if len(item.ValidDate) >= 10 {
			item.ValidDate = item.ValidDate[:10]
		} else {
			item.ValidDate = ""
		}

		result[item.GoodsId] = item
	}

	return result, nil
}

func GetClient(ctx *context.Context, config EcomConfig) *EcomClient {
	return &EcomClient{
		config:  config,
		ctx:     ctx,
		limiter: rate.NewLimiter(rate.Limit(connectionLimit), connectionLimit+1),
	}
}
