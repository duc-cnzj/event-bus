package conn

import (
	"context"
	log "github.com/sirupsen/logrus"
	"time"

	"github.com/streadway/amqp"
)

var conn *amqp.Connection

func NewConn(url string) (*amqp.Connection, error) {
	var err error
	conn, err = amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func ReConnect(url string) *amqp.Connection {
	var err error
	log.Warn("reconnect")
	timeout, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
LABEL:
	for {
		select {
		case <-timeout.Done():
			log.Fatalln("rabbitmq reconnect timeout 60s.")
		case <-time.After(1 * time.Second):
			conn, err = NewConn(url)
			if err == nil {
				log.Info("success")
				break LABEL
			}
			log.Debug(err)
		}
	}

	return conn
}
