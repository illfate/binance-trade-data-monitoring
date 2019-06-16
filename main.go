package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/adshao/go-binance"
	_ "github.com/illfate/binance-trade-data-monitoring/tectonic"
)

// Depth represents a binance depth
type Depth struct {
	LastUpdateID int         `json:"lastUpdateId"`
	Bids         [][2]string `json:"bids"`
	Asks         [][2]string `json:"asks"`
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

	wsDepthHandler := func(event *binance.WsDepthEvent) {
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
