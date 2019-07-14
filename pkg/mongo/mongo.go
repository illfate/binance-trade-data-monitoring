package mongo

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
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

func New(database, dbCollection, uri string) (*Mongo, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
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
	collection := client.Database(database).Collection(dbCollection)
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

func (m *Mongo) StartDepthReq(ctx context.Context, symbol string, errHandler func(err error)) {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				err := m.getRequestDepth(symbol)
				if err != nil {
					errHandler(err)
				}
			case <-ctx.Done():
				ticker.Stop()
				log.Printf("StartDepthReq(%s) has stopped", symbol)
				return
			}
		}
	}()
}
