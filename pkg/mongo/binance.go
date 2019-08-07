package mongo

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func (db *DB) StartDepthReq(ctx context.Context, symbol string, errHandler func(err error)) {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				err := db.getRequestDepth(symbol)
				if err != nil {
					errHandler(errors.Wrap(err, "couldn't get depth request"))
				}
			case <-ctx.Done():
				ticker.Stop()
				log.Printf("StartDepthReq(%s) has stopped", symbol)
				return
			}
		}
	}()
}

func (db *DB) getRequestDepth(symbol string) error {
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

	_, err = db.InsertOne(context.TODO(), body, nil)
	if err != nil {
		return err
	}
	return nil
}
