package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/illfate/binance-trade-data-monitoring/pkg/tectonic"
)

func main() {
	t, err := tectonic.NewTectonic()
	if err != nil {
		log.Print(err)
		return
	}

	m, err := NewMongo()
	if err != nil {
		log.Print(err)
		return
	}

	errors := make(chan error)
	errHandler := func(err error) {
		select {
		case errors <- err:
		default:
		}
	}
	m.startDepthReq("ETHBTC", errHandler)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	err = t.processBinance(ctx, &wg, "ETHBTC", errHandler)
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
