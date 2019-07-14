package tectonic

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/adshao/go-binance"
)

// Tectonic holds tectonic db
type DB struct {
	conn *Tectonic
}

// NewTectonic creates new server
// IP port "127.0.0.1" 9002
func New(IP, port string) (*DB, error) {
	portParsed, err := strconv.ParseUint(port, 10, 16)
	db := NewTectonic(IP, uint16(portParsed))
	err = db.Connect()
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
	return &DB{
		conn: db,
	}, nil
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
