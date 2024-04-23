package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CryptoData struct {
	Amount    float64   `json:"amount"`
	Base      string    `json:"base"`
	Currency  string    `json:"currency"`
	FetchDate time.Time `bson:"fetchDate"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://admin:password@localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	collection := client.Database("cryptoDB").Collection("prices")

	startDate, _ := time.Parse("2006-01-02", "2023-01-01")
	endDate, _ := time.Parse("2006-01-02", "2024-04-01")
	oneWeek := time.Hour * 24 * 7

	for current := startDate; current.Before(endDate); current = current.Add(oneWeek) {
		_, weekOfYear := current.ISOWeek()
		count, err := collection.CountDocuments(ctx, bson.M{"fetchDate": current})
		if err != nil {
			log.Fatal(err)
		}
		if count == 0 {
			data, err := fetchCryptoData(current.Format("2006-01-02"))
			if err != nil {
				log.Fatal(err)
			}
			data.FetchDate = current

			_, err = collection.InsertOne(ctx, data)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Data for week %d of %d added.\n", weekOfYear, current.Year())
			fmt.Println("sleeping for 3 seconds")
			time.Sleep(3 * time.Second)
		} else {
			fmt.Printf("Already added Data for week %d of %d.\n", weekOfYear, current.Year())
		}
	}

	pipeline := mongo.Pipeline{
		{{"$group", bson.D{{"_id", nil}, {"averagePrice", bson.D{{"$avg", "$amount"}}}}}},
	}

	cur, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(ctx)

	var results []bson.M
	if err = cur.All(ctx, &results); err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		fmt.Printf("Average Price: %v USD\n", result["averagePrice"])
	}
}

func fetchCryptoData(date string) (*CryptoData, error) {
	url := fmt.Sprintf("https://api.coinbase.com/v2/prices/btc-usd/spot?date=%s", date)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var apiResponse struct {
		Data struct {
			Amount   string `json:"amount"`
			Base     string `json:"base"`
			Currency string `json:"currency"`
		} `json:"data"`
	}
	if err = json.Unmarshal(body, &apiResponse); err != nil {
		return nil, err
	}

	amount, err := strconv.ParseFloat(apiResponse.Data.Amount, 64)
	if err != nil {
		return nil, err
	}

	return &CryptoData{
		Amount:   amount,
		Base:     apiResponse.Data.Base,
		Currency: apiResponse.Data.Currency,
	}, nil
}
