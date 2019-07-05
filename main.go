package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/adshao/go-binance"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/illfate/binance-trade-data-monitoring/tectonic"
)

// Depth represents a binance depth
type Depth struct {
	LastUpdateID int         `json:"lastUpdateId"`
	Bids         [][2]string `json:"bids"`
	Asks         [][2]string `json:"asks"`
}

// Tectonic holds tectonic db
type Tectonic struct {
	DB *tectonic.Tectonic
}

type Mongo struct {
	DB *mongo.Collection
}

// NewTectonic creates new server
func NewTectonic() (*Tectonic, error) {
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
	return &Tectonic{
		DB: db,
	}, nil
}

func NewMongo() (*Mongo, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	if err != nil {
		return nil, err
	}
	err = client.Connect(context.TODO())
	if err != nil {
		return nil, err
	}

	// Check the connection
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

func (m *Mongo) startDepthReq() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			err := m.getRequestDepth("ETHBTC")
			if err != nil {
				log.Print(err)
				return
			}
		}
	}()
}

func main() {
	t, err := NewTectonic()
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

func (t *Tectonic) processBinance(ctx context.Context, wg *sync.WaitGroup,
	symbol string, errHandler binance.ErrHandler) error {

	wg.Add(1)
	err := t.startDepthServe(ctx, wg, symbol, errHandler)
	if err != nil {
		return err
	}

	//_, _, err = t.processTrade(symbol)
	//if err != nil {
	//	return err
	//}
	return nil
}

func (t *Tectonic) startDepthServe(ctx context.Context, wg *sync.WaitGroup,
	symbol string, errHandler binance.ErrHandler) error {

	wsDepthHandler := func(event *binance.WsDepthEvent) {
		err := t.insertBids(event)
		if err != nil {
			errHandler(err)
			return
		}
		err = t.insertAsks(event)
		if err != nil {
			errHandler(err)
			return
		}
	}

	done, stop, err := binance.WsDepthServe(symbol, wsDepthHandler, errHandler)
	if err != nil {
		wg.Done()
		return fmt.Errorf("couldn't strart listening websockert: %s", err)
	}

	go func() {
		defer wg.Done()

		select {
		case <-ctx.Done():
			close(stop)
		case <-done:
			return
		}
	}()

	return nil
}

func (t *Tectonic) processTrade(symbol string) (doneC, stopC chan struct{}, err error) {
	errHandler := func(err error) {
		fmt.Println(err)
	}
	wsTradeHandler := func(event *binance.WsTradeEvent) {
		price, err := strconv.ParseFloat(event.Price, 64)
		if err != nil {
			log.Fatal(fmt.Errorf("cannot parse price to flaot: %s", err))
		}
		qty, err := strconv.ParseFloat(event.Quantity, 64)
		if err != nil {
			log.Fatal(fmt.Errorf("cannot parse price to flaot: %s", err))
		}
		delta := tectonic.Delta{
			Timestamp: float64(event.TradeTime),
			Price:     price,
			Size:      qty,
			Seq:       uint32(event.TradeID),
			IsTrade:   true,
			IsBid:     false,
		}
		err = t.DB.Insert(&delta)
	}
	return binance.WsTradeServe("ETHBTC", wsTradeHandler, errHandler)
}

func (t *Tectonic) insertAsks(event *binance.WsDepthEvent) error {
	for _, ask := range event.Asks {
		price, err := strconv.ParseFloat(ask.Price, 64)
		if err != nil {
			return fmt.Errorf("cannot parse price to flaot: %s", err)
		}
		qty, err := strconv.ParseFloat(ask.Quantity, 64)
		if err != nil {
			return fmt.Errorf("cannot parse qty to flaot: %s", err)
		}
		delta := tectonic.Delta{
			Timestamp: float64(event.Time),
			Price:     price,
			Size:      qty,
			Seq:       uint32(event.FirstUpdateID),
			IsTrade:   false,
			IsBid:     true,
		}
		err = t.DB.Insert(&delta)
		if err != nil {
			return fmt.Errorf("could not insert into db: %s", err)
		}
	}
	return nil
}

func (t *Tectonic) insertBids(event *binance.WsDepthEvent) error {
	for _, bid := range event.Bids {
		price, err := strconv.ParseFloat(bid.Price, 64)
		if err != nil {
			return fmt.Errorf("cannot parse price to flaot: %s", err)
		}
		qty, err := strconv.ParseFloat(bid.Quantity, 64)
		if err != nil {
			return fmt.Errorf("cannot parse qty to flaot: %s", err)
		}
		delta := tectonic.Delta{
			Timestamp: float64(event.Time),
			Price:     price,
			Size:      qty,
			Seq:       uint32(event.FirstUpdateID),
			IsTrade:   false,
			IsBid:     true,
		}
		err = t.DB.Insert(&delta)
		if err != nil {
			return fmt.Errorf("could not insert into db: %s", err)
		}
	}
	return nil
}
