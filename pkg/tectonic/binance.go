package tectonic

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/pkg/errors"

	tectonic "github.com/rickyhan/tectonicdb/cli/golang"

	"github.com/adshao/go-binance"
)

// ProcessBinance starts listening depth and trade sockets and write info to db.
func (db *DB) ProcessBinance(ctx context.Context, wg *sync.WaitGroup,
	symbol string, errHandler binance.ErrHandler) error {

	wg.Add(1)
	err := db.startDepthServe(ctx, wg, symbol, errHandler)
	if err != nil {
		return err
	}

	wg.Add(1)
	err = db.startTrade(ctx, wg, symbol, errHandler)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) startDepthServe(ctx context.Context, wg *sync.WaitGroup,
	symbol string, errHandler binance.ErrHandler) error {

	wsDepthHandler := func(event *binance.WsDepthEvent) {
		err := db.insertBids(event)
		if err != nil {
			errHandler(errors.Wrap(err, "couldn't insert bids"))
			return
		}
		err = db.insertAsks(event)
		if err != nil {
			errHandler(errors.Wrap(err, "couldn't insert asks"))
			return
		}
	}

	done, stop, err := binance.WsDepthServe(symbol, wsDepthHandler, errHandler)
	if err != nil {
		wg.Done()
		return errors.Wrap(err, "couldn't strart listening websockert")
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

func (db *DB) startTrade(ctx context.Context, wg *sync.WaitGroup, symbol string,
	errHandler binance.ErrHandler) (err error) {

	wsTradeHandler := func(event *binance.WsTradeEvent) {
		price, err := strconv.ParseFloat(event.Price, 64)
		if err != nil {
			log.Printf("couldn't parse price to float: %s", err)
			errHandler(err)
			return
		}
		qty, err := strconv.ParseFloat(event.Quantity, 64)
		if err != nil {
			log.Printf("couldn't parse price to float: %s", err)
			errHandler(err)
			return
		}
		delta := tectonic.Delta{
			Timestamp: float64(event.TradeTime),
			Price:     price,
			Size:      qty,
			Seq:       uint32(event.TradeID),
			IsTrade:   true,
			IsBid:     false,
		}
		err = db.conn.Insert(&delta)
		if err != nil {
			log.Printf("couldn't insert into db: %s", err)
		}
	}
	done, stop, err := binance.WsTradeServe(symbol, wsTradeHandler, errHandler)
	if err != nil {
		wg.Done()
		return errors.Wrap(err, "couldn't start listening websocket")
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

func (db *DB) insertAsks(event *binance.WsDepthEvent) error {
	for _, ask := range event.Asks {
		price, err := strconv.ParseFloat(ask.Price, 64)
		if err != nil {
			return errors.Wrapf(err, "couldn't parse ask price to float [%s]", ask.Price)
		}
		qty, err := strconv.ParseFloat(ask.Quantity, 64)
		if err != nil {
			return errors.Wrapf(err, "couldn't parse ask qty to float [%s]", ask.Quantity)
		}
		delta := tectonic.Delta{
			Timestamp: float64(event.Time),
			Price:     price,
			Size:      qty,
			Seq:       uint32(event.FirstUpdateID),
			IsTrade:   false,
			IsBid:     true,
		}
		err = db.conn.Insert(&delta)
		if err != nil {
			return errors.Wrap(err, "couldn't insert delta asks into db")
		}
	}
	return nil
}

func (db *DB) insertBids(event *binance.WsDepthEvent) error {
	for _, bid := range event.Bids {
		price, err := strconv.ParseFloat(bid.Price, 64)
		if err != nil {
			return errors.Wrapf(err, "couldn't parse bid price to float [%s]", bid.Price)
		}
		qty, err := strconv.ParseFloat(bid.Quantity, 64)
		if err != nil {
			return errors.Wrapf(err, "couldn't parse bid qty to float [%s]", bid.Quantity)
		}
		delta := tectonic.Delta{
			Timestamp: float64(event.Time),
			Price:     price,
			Size:      qty,
			Seq:       uint32(event.FirstUpdateID),
			IsTrade:   false,
			IsBid:     true,
		}
		err = db.conn.Insert(&delta)
		if err != nil {
			return fmt.Errorf("coudln't insert delta bids into db: %s", err)
		}
	}
	return nil
}
