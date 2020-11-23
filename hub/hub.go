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
	"runtime"
	"sync"
	"time"
)

var producerCreateMu sync.RWMutex
var consumerCreateMu sync.RWMutex
var NilConnError = errors.New("conn is nil")

type Interface interface {
	ConsumerManager() ConsumerManagerInterface
	ProducerManager() ProducerManagerInterface

	NewProducer(queueName, kind string) (ProducerInterface, error)
	NewConsumer(queueName, kind string) (ConsumerInterface, error)

	RegisterProducer(p ProducerInterface)
	UnRegisterProducer(p ProducerInterface)

	RegisterConsumer(c ConsumerInterface)
	UnRegisterConsumer(c ConsumerInterface)

	GetAmqpConn() (*amqp.Connection, error)
	GetDBConn() *gorm.DB

	IsClosed() bool
	Close()

	CloseAllConsumer()
	CloseAllProducer()

	Done() <-chan struct{}
	AmqpConnDone() <-chan *amqp.Error

	Config() *config.Config
}

type Hub struct {
	Conn      *amqp.Connection
	Producers map[string]ProducerInterface
	Consumers map[string]ConsumerInterface
	DB        *gorm.DB

	pMu sync.RWMutex
	cMu sync.RWMutex

	pm ProducerManagerInterface
	cm ConsumerManagerInterface

	ctx    context.Context
	cancel context.CancelFunc

	closed bool

	cfg *config.Config

	NotifyConnClose chan *amqp.Error
}

func (h *Hub) GetDBConn() *gorm.DB {
	return h.DB
}

func (h *Hub) ConsumerManager() ConsumerManagerInterface {
	return h.cm
}

func (h *Hub) ProducerManager() ProducerManagerInterface {
	return h.pm
}

func NewHub(conn *amqp.Connection, cfg *config.Config, db *gorm.DB) Interface {
	cancel, cancelFunc := context.WithCancel(context.Background())
	h := &Hub{
		DB:              db,
		cfg:             cfg,
		Conn:            conn,
		Producers:       map[string]ProducerInterface{},
		Consumers:       map[string]ConsumerInterface{},
		NotifyConnClose: conn.NotifyClose(make(chan *amqp.Error)),
		ctx:             cancel,
		cancel:          cancelFunc,
	}
	h.pm = NewProducerManager(h)
	h.cm = NewConsumerManager(h)

	if log.IsLevelEnabled(log.DebugLevel) {
		go func() {
			for {
				select {
				case <-time.After(1 * time.Second):
					log.Debugf("Consumers: %d, Producers: %d, NumGoroutine: %d\n", len(h.Consumers), len(h.Producers), runtime.NumGoroutine())
				case <-h.ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		for {
			select {
			case <-h.NotifyConnClose:
				log.Debug("hub amqp connection notify close....")
				log.Debug("start to CloseAllProducer")
				h.CloseAllProducer()
				log.Debug("start to CloseAllConsumer")
				h.CloseAllConsumer()
				h.Producers = map[string]ProducerInterface{}
				h.Consumers = map[string]ConsumerInterface{}
				h.Conn = nil
				log.Debug("start to ReConnect")
				h.Conn = conn2.ReConnect(h.Config().AmqpUrl)
				h.NotifyConnClose = h.Conn.NotifyClose(make(chan *amqp.Error))
			case <-h.ctx.Done():
				log.Info("hub ctx Done exit")
				return
			}
		}
	}()

	return h
}

func (h *Hub) AmqpConnDone() <-chan *amqp.Error {
	return h.NotifyConnClose
}
func (h *Hub) getKey(queueName, kind string) string {
	return queueName + "@" + kind
}

func (h *Hub) NewProducer(queueName, kind string) (ProducerInterface, error) {
	producerCreateMu.Lock()
	defer producerCreateMu.Unlock()
	var (
		producer ProducerInterface
		err      error
		ok       bool
	)

	if producer, ok = h.getRegisterProducer(queueName, kind, producer, ok); ok {
		return producer, nil
	}

	if producer, err = h.ProducerManager().GetProducer(queueName, kind); err != nil {
		return nil, err
	}

	h.RegisterProducer(producer)

	return producer, nil
}

func (h *Hub) getRegisterProducer(queueName string, kind string, producer ProducerInterface, ok bool) (ProducerInterface, bool) {
	h.pMu.RLock()
	defer h.pMu.RUnlock()
	producer, ok = h.Producers[h.getKey(queueName, kind)]
	return producer, ok
}

func (h *Hub) NewConsumer(queueName, kind string) (ConsumerInterface, error) {
	consumerCreateMu.Lock()
	defer consumerCreateMu.Unlock()
	var (
		consumer ConsumerInterface
		err      error
		ok       bool
	)

	if consumer, ok = h.getRegisterConsumer(queueName, kind, consumer, ok); ok {
		return consumer, nil
	}

	if consumer, err = h.ConsumerManager().GetConsumer(queueName, kind); err != nil {
		return nil, err
	}

	h.RegisterConsumer(consumer)

	return consumer, nil
}

func (h *Hub) getRegisterConsumer(queueName string, kind string, consumer ConsumerInterface, ok bool) (ConsumerInterface, bool) {
	h.cMu.RLock()
	defer h.cMu.RUnlock()
	consumer, ok = h.Consumers[h.getKey(queueName, kind)]
	return consumer, ok
}

func (h *Hub) Done() <-chan struct{} {
	return h.ctx.Done()
}

func (h *Hub) RegisterProducer(p ProducerInterface) {
	h.pMu.Lock()
	defer h.pMu.Unlock()
	if h.Producers == nil {
		h.Producers = map[string]ProducerInterface{}
	}

	h.Producers[h.getKey(p.GetQueueName(), p.GetKind())] = p
}

func (h *Hub) UnRegisterProducer(p ProducerInterface) {
	h.pMu.Lock()
	defer h.pMu.Unlock()
	if h.Producers == nil {
		return
	}

	delete(h.Producers, h.getKey(p.GetQueueName(), p.GetKind()))
}

func (h *Hub) RegisterConsumer(c ConsumerInterface) {
	h.cMu.Lock()
	defer h.cMu.Unlock()

	if h.Consumers == nil {
		h.Consumers = map[string]ConsumerInterface{}
	}

	h.Consumers[h.getKey(c.GetQueueName(), c.GetKind())] = c
}

func (h *Hub) UnRegisterConsumer(c ConsumerInterface) {
	h.cMu.Lock()
	defer h.cMu.Unlock()

	if h.Consumers == nil {
		return
	}

	delete(h.Consumers, h.getKey(c.GetQueueName(), c.GetKind()))
}

func (h *Hub) GetAmqpConn() (*amqp.Connection, error) {
	if h.Conn != nil && !h.Conn.IsClosed() {
		return h.Conn, nil
	}

	return nil, NilConnError
}

func (h *Hub) IsClosed() bool {
	return h.closed
}

func (h *Hub) Close() {
	var (
		db  *sql.DB
		err error
	)
	log.Info("hub closing.")
	h.cancel()
	h.closed = true
	log.Info("hub canceled.")
	if !h.Conn.IsClosed() && h.Conn != nil {
		h.CloseAllProducer()
		log.Info("hub producer closed.")
		h.CloseAllConsumer()
		log.Info("hub consumer closed.")

		if err = h.Conn.Close(); err != nil {
			log.Error(err)
		}
		log.Info("hub amqp conn closed.")
	}

	if db, err = h.DB.DB(); err != nil {
		log.Error(err)
	}
	if err = db.Close(); err != nil {
		log.Error(err)
	}

	log.Info("sql db closed.")

	log.Info("hub closed.")
}

func (h *Hub) CloseAllConsumer() {
	for i, consumer := range h.Consumers {
		log.Info("h.consumer before close ", i)
		consumer.Close()
		log.Info("h.consumer before close ", i)
	}
}

func (h *Hub) CloseAllProducer() {
	for i, producer := range h.Producers {
		log.Info("h.Producers before close ", i)
		producer.Close()
		log.Info("h.Producers after close ", i)
	}
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
