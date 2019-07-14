package mongo

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Depth represents a binance depth
type Depth struct {
	LastUpdateID int         `json:"lastUpdateId"`
	Bids         [][2]string `json:"bids"`
	Asks         [][2]string `json:"asks"`
}

type Mongo struct {
	DB *mongo.Collection
}

func NewMongo(database, collectiob, uri string) (*Mongo, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	if err != nil {
		return nil, err
	}
	err = client.Connect(context.TODO())
	if err != nil {
		return nil, err
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return nil, err
	}
	collection := client.Database("binance").Collection("depth")
	return &Mongo{
		DB: collection,
	}, nil
}

func (m *Mongo) getRequestDepth(symbol string) error {
	req, err := http.NewRequest("GET", "https://www.binance.com/api/v1/depth", nil)
	if err != nil {
		return fmt.Errorf("could not create request: %s", err)
	}
	q := req.URL.Query()
	q.Add("symbol", symbol)
	q.Add("limit", "1000")
	req.URL.RawQuery = q.Encode()
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("could not send request: %s", err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read from body: %s", err)
	}

	_, err = m.DB.InsertOne(context.TODO(), body, nil)
	if err != nil {
		return err
	}
	return nil
}

func (m *Mongo) startDepthReq(symbol string, errHandler func(err error)) {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		select {
		case <-ticker.C:
			err := m.getRequestDepth(symbol)
			if err != nil {
				errHandler(err)
			}
		}
	}()
}
