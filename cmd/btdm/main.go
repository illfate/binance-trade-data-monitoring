package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/illfate/binance-trade-data-monitoring/pkg/mongo"

	"github.com/illfate/binance-trade-data-monitoring/pkg/tectonic"
)

func main() {
	tPort := os.Getenv("TECTONIC_PORT")
	tIP := os.Getenv("TECTONIC_IP")
	tDBName := os.Getenv("TECTONIC_DB_NAME")
	tDB, err := tectonic.New(tIP, tPort, tDBName)
	if err != nil {
		log.Printf("couldn't connect to tectonic db: %s", err)
		return
	}
	mongoDB := os.Getenv("MONGO_DB")
	mongoURI := os.Getenv("MONGO_URI")
	mongoCollection := os.Getenv("MONGO_COLLECTION")
	m, err := mongo.New(mongoDB, mongoCollection, mongoURI)
	if err != nil {
		log.Printf("couldn't start mongo db: %s", err)
		return
	}

	errors := make(chan error)
	errHandler := func(err error) {
		select {
		case errors <- err:
		default:
		}
	}
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	m.StartDepthReq(ctx, "ETHBTC", errHandler)

	err = tDB.ProcessBinance(ctx, &wg, "ETHBTC", errHandler)
	if err != nil {
		log.Print(err)
		return
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	select {
	case err := <-errors:
		log.Print(err)
		cancel()

	case <-sigs:
		log.Printf("stopped...")
		cancel()
	}

	wg.Wait()
}
