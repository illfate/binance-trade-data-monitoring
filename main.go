package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"

	"github.com/adshao/go-binance"

	"github.com/illfate/binance-trade-data-monitoring/tectonic"
)

// Depth represents a binance depth
type Depth struct {
	LastUpdateID int         `json:"lastUpdateId"`
	Bids         [][2]string `json:"bids"`
	Asks         [][2]string `json:"asks"`
}

func setUpDB() (*tectonic.Tectonic, error) {
	db := tectonic.NewTectonic("127.0.0.1", 9002)
	err := db.Connect()
	if err != nil {
		return nil, fmt.Errorf("could not connect to tectonic: %s", err)
	}
	err = db.Create("binance")
	if err != nil {
		return nil, fmt.Errorf("could not create tectonic db: %s", err)
	}
	err = db.Use("binance")
	if err != nil {
		return nil, fmt.Errorf("could not switch to db: %s", err)
	}
	return db, nil
}

func main() {
	req, err := http.NewRequest("GET", "https://www.binance.com/api/v1/depth", nil)
	if err != nil {
		log.Printf("could not create request: %s", err)
		return
	}
	q := req.URL.Query()
	q.Add("symbol", "ETHBTC")
	q.Add("limit", "1000")
	req.URL.RawQuery = q.Encode()
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("could not send request: %s", err)
		return
	}
	defer resp.Body.Close()
	if err != nil {
		log.Printf("could not read from body: %s", err)
		return
	}
	var depth Depth
	json.NewDecoder(resp.Body).Decode(&depth)
	fmt.Printf("%+v\n", depth)

	db, err := setUpDB()
	if err != nil {
		log.Fatal(err)
	}
	wsDepthHandler := func(event *binance.WsDepthEvent) {
		err = insertBids(db, event)
		if err != nil {
			log.Fatal(err)
		}
		err = insertAsks(db, event)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(*event)
	}
	errHandler := func(err error) {
		fmt.Println(err)
	}
	_, stop, err := binance.WsDepthServe("ETHBTC", wsDepthHandler, errHandler)
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, os.Interrupt)
		<-sigs
		log.Printf("stopped...\n")
		stop <- struct{}{}
	}()
	<-stop
}

func insertAsks(db *tectonic.Tectonic, event *binance.WsDepthEvent) error {
	for _, ask := range event.Asks {
		price, err := strconv.ParseFloat(ask.Price, 64)
		if err != nil {
			return fmt.Errorf("cannot parse price to flaot: %s", err)
		}
		qty, err := strconv.ParseFloat(ask.Quantity, 64)
		if err != nil {
			return fmt.Errorf("cannot parse price to flaot: %s", err)
		}
		delta := tectonic.Delta{
			Timestamp: float64(event.Time),
			Price:     price,
			Size:      qty,
			Seq:       uint32(event.FirstUpdateID),
			IsTrade:   false,
			IsBid:     true,
		}
		err = db.Insert(&delta)
		if err != nil {
			return fmt.Errorf("could not insert into db: %s", err)
		}
	}
	return nil
}

func insertBids(db *tectonic.Tectonic, event *binance.WsDepthEvent) error {
	for _, bid := range event.Bids {
		price, err := strconv.ParseFloat(bid.Price, 64)
		if err != nil {
			return fmt.Errorf("cannot parse price to flaot: %s", err)
		}
		qty, err := strconv.ParseFloat(bid.Quantity, 64)
		if err != nil {
			return fmt.Errorf("cannot parse price to flaot: %s", err)
		}
		delta := tectonic.Delta{
			Timestamp: float64(event.Time),
			Price:     price,
			Size:      qty,
			Seq:       uint32(event.FirstUpdateID),
			IsTrade:   false,
			IsBid:     true,
		}
		err = db.Insert(&delta)
		if err != nil {
			return fmt.Errorf("could not insert into db: %s", err)
		}
	}
	return nil
}
