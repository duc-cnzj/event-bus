package hub

import (
	"context"
	"database/sql"
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"gorm.io/gorm"
	"mq/config"
	conn2 "mq/conn"
	"mq/models"
	"sync/atomic"
	"time"
)

var AmqpConnClosed = errors.New("amqp conn closed")

type atomicBool int32

func (b *atomicBool) isSet() bool { return atomic.LoadInt32((*int32)(b)) != 0 }
func (b *atomicBool) setTrue()    { atomic.StoreInt32((*int32)(b), 1) }
func (b *atomicBool) setFalse()   { atomic.StoreInt32((*int32)(b), 0) }

type Interface interface {
	ConsumerManager() ConsumerManagerInterface
	ProducerManager() ProducerManagerInterface

	NewProducer(queueName, kind string) (ProducerInterface, error)
	NewConsumer(queueName, kind string) (ConsumerInterface, error)

	RemoveProducer(p ProducerInterface)
	RemoveConsumer(c ConsumerInterface)

	CloseAllConsumer()
	CloseAllProducer()

	GetAmqpConn() (*amqp.Connection, error)
	GetDBConn() *gorm.DB

	IsClosed() bool
	Close()

	Done() <-chan struct{}
	AmqpConnDone() <-chan *amqp.Error

	Config() *config.Config
}

type Hub struct {
	amqpConn *amqp.Connection
	db       *gorm.DB

	pm ProducerManagerInterface
	cm ConsumerManagerInterface

	ctx    context.Context
	cancel context.CancelFunc

	closed atomicBool

	cfg *config.Config

	notifyConnClose chan *amqp.Error
}

func NewHub(conn *amqp.Connection, cfg *config.Config, db *gorm.DB) Interface {
	cancel, cancelFunc := context.WithCancel(context.Background())
	h := &Hub{
		db:              db,
		cfg:             cfg,
		amqpConn:        conn,
		notifyConnClose: conn.NotifyClose(make(chan *amqp.Error)),
		ctx:             cancel,
		cancel:          cancelFunc,
	}
	h.pm = NewProducerManager(h)
	h.cm = NewConsumerManager(h)

	go func() {
		for {
			select {
			case <-h.AmqpConnDone():
				if h.IsClosed() {
					return
				}
				log.Error("amqp 连接断开")
				h.amqpConn.Close()
				log.Error("amqp 开始重连")
				h.amqpConn = conn2.ReConnect(h.Config().AmqpUrl)
				h.notifyConnClose = h.amqpConn.NotifyClose(make(chan *amqp.Error))
			case <-h.ctx.Done():
				log.Info("hub ctx Done exit")
				return
			}
		}
	}()

	return h
}

func (h *Hub) AmqpConnDone() <-chan *amqp.Error {
	return h.notifyConnClose
}

func (h *Hub) GetDBConn() *gorm.DB {
	return h.db
}

func (h *Hub) ProducerManager() ProducerManagerInterface {
	return h.pm
}

func (h *Hub) ConsumerManager() ConsumerManagerInterface {
	return h.cm
}

func (h *Hub) NewProducer(queueName, kind string) (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if producer, err = h.ProducerManager().GetProducer(queueName, kind); err != nil {
		return nil, err
	}

	return producer, nil
}

func (h *Hub) NewConsumer(queueName, kind string) (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	if consumer, err = h.ConsumerManager().GetConsumer(queueName, kind); err != nil {
		return nil, err
	}

	return consumer, nil
}

func (h *Hub) RemoveProducer(p ProducerInterface) {
	h.ProducerManager().RemoveProducer(p)
}

func (h *Hub) RemoveConsumer(c ConsumerInterface) {
	h.ConsumerManager().RemoveConsumer(c)
}

func (h *Hub) CloseAllConsumer() {
	h.ConsumerManager().CloseAll()
}

func (h *Hub) CloseAllProducer() {
	h.ProducerManager().CloseAll()
}

func (h *Hub) Done() <-chan struct{} {
	return h.ctx.Done()
}

func (h *Hub) GetAmqpConn() (*amqp.Connection, error) {
	if !h.amqpConn.IsClosed() {
		return h.amqpConn, nil
	}

	return nil, AmqpConnClosed
}

func (h *Hub) IsClosed() bool {
	return h.closed.isSet()
}

func (h *Hub) Close() {
	var (
		db  *sql.DB
		err error
	)
	if h.IsClosed() {
		return
	}
	h.closed.setTrue()
	log.Info("hub closing.")
	h.cancel()
	log.Info("hub canceled.")
	if !h.amqpConn.IsClosed() {
		h.CloseAllProducer()
		log.Info("hub producer closed.")
		h.CloseAllConsumer()
		log.Info("hub consumer closed.")

		if err = h.amqpConn.Close(); err != nil {
			log.Error(err)
		}
		log.Info("hub amqp conn closed.")
	}

	if db, err = h.db.DB(); err != nil {
		log.Error(err)
	}
	if err = db.Close(); err != nil {
		log.Error(err)
	}

	log.Info("sql db closed.")

	log.Info("hub closed.")
}

func (h *Hub) Config() *config.Config {
	return h.cfg
}

func Ack(db *gorm.DB, queueId uint64) error {
	if queueId == 0 {
		log.Debug(queueId)
		return nil
	}
	if err := db.Delete(&models.Queue{ID: uint(queueId)}).Error; err != nil {
		return err
	}

	return nil
}

func Nack(db *gorm.DB, queueId uint64) error {
	if queueId == 0 {
		log.Debug(queueId)
		return nil
	}
	var queue = &models.Queue{ID: uint(queueId)}
	if err := db.Find(&queue).Error; err != nil {
		return err
	}

	now := time.Now()

	return db.Transaction(func(tx *gorm.DB) error {
		tx.Delete(queue)

		tx.Create(&models.Queue{
			RetryTimes: queue.RetryTimes + 1,
			Data:       queue.Data,
			QueueName:  queue.QueueName,
			Ref:        int(queue.ID),
			RunAfter:   &now,
		})
		return nil
	})
}

func DelayPublish(db *gorm.DB, queueName string, msg Message) (*models.Queue, error) {
	startTime := time.Now().Add(time.Duration(msg.DelaySeconds) * time.Second)

	queue := &models.Queue{
		DeletedAt:    gorm.DeletedAt{},
		RunAfter:     &startTime,
		DelaySeconds: msg.DelaySeconds,
		Data:         msg.Data,
		QueueName:    queueName,
	}
	if err := db.Create(queue).Error; err != nil {
		return nil, err
	}

	log.Debug(queue)
	return queue, nil
}
