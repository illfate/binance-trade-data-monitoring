package mongo

import (
	"context"
	"github.com/pkg/errors"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Depth represents a binance depth
type Depth struct {
	LastUpdateID int         `json:"lastUpdateId"`
	Bids         [][2]string `json:"bids"`
	Asks         [][2]string `json:"asks"`
}

type DB struct {
	*mongo.Collection
}

func New(database, dbCollection, uri string) (*DB, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create new mongo client")
	}
	err = client.Connect(context.TODO())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't connect to mongo db")
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't ping mongo db")
	}
	collection := client.Database(database).Collection(dbCollection)
	return &DB{
		Collection: collection,
	}, nil
}
