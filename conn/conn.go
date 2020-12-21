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
	log.Warn("reconnecting")
	timeout, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

LABEL:
	for {
		select {
		case <-timeout.Done():
			log.Fatalln("rabbitmq reconnect timeout 60s.")
		case <-ticker.C:
			conn, err = NewConn(url)
			if err == nil {
				log.Info("reconnected success")
				break LABEL
			}
			log.Debug(err)
		}
	}

	return conn
}
