package tectonic

import (
	"github.com/pkg/errors"
	tectonic "github.com/rickyhan/tectonicdb/cli/golang"
	"strconv"
)

// Tectonic holds tectonic db
type DB struct {
	conn *tectonic.Tectonic
}

// NewTectonic creates new server
func New(ip, port, dbName string) (*DB, error) {
	portParsed, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse port string")
	}
	db := &tectonic.Tectonic{

		Host: ip,
		Port: uint16(portParsed),
	}
	err = db.Connect()
	if err != nil {
		return nil, errors.Wrap(err, "couldn't connect to tectonic")
	}
	err = db.Use(dbName)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't switch to db")
	}
	return &DB{
		conn: db,
	}, nil
}
