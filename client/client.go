package main

import (
	"fmt"
	"log"

	"github.com/illfate/binance-trade-data-monitoring/tectonic"
)

func main() {
	db := tectonic.NewTectonic("127.0.0.1", 9002)
	err := db.Connect()
	if err != nil {
		log.Fatalf("could not connect to tectonic: %s", err)
	}

	err = db.Use("binance")
	if err != nil {
		log.Fatalf("could not switch to db: %s", err)
	}
	res, err := db.Info()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res)
	delta, err := db.Get(13)
	if err != nil {
		log.Fatal(err)
	}
	for idx, d := range *delta {
		fmt.Printf("%v) %+v\n", idx, d)
	}
}
