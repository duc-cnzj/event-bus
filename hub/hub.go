package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"mq/config"
	conn2 "mq/conn"
	"mq/models"
	"sync/atomic"
	"time"
)

var AmqpConnClosed = errors.New("amqp conn closed")
var AckQueueName = "event_bus_ack_queue"
var ConfirmQueueName = "event_bus_confirm_queue"

var _ Interface = (*Hub)(nil)

type atomicBool int32

func (b *atomicBool) isSet() bool { return atomic.LoadInt32((*int32)(b)) != 0 }
func (b *atomicBool) setTrue()    { atomic.StoreInt32((*int32)(b), 1) }
func (b *atomicBool) setFalse()   { atomic.StoreInt32((*int32)(b), 0) }

type Interface interface {
	ConsumeConfirmQueue()
	ConsumeAckQueue()

	GetConfirmProducer() (ProducerInterface, error)
	GetConfirmConsumer() (ConsumerInterface, error)

	GetAckQueueProducer() (ProducerInterface, error)
	GetAckQueueConsumer() (ConsumerInterface, error)

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
				go h.ConsumeConfirmQueue()
				go h.ConsumeAckQueue()
			case <-h.ctx.Done():
				log.Info("hub ctx Done exit")
				return
			}
		}
	}()

	return h
}

func (h *Hub) ConsumeConfirmQueue() {
	var (
		err      error
		consumer ConsumerInterface
	)

	defer func() {
		log.Error("ConsumeConfirmQueue EXIT")
	}()

	if consumer, err = h.GetConfirmConsumer(); err != nil {
		log.Error("err ConsumeConfirmQueue()", err)
		return
	}

	defer consumer.Close()

	for {
		select {
		case <-consumer.Done():
			return
		case <-h.Done():
			log.Error("hub done ConsumeConfirmQueue exit.")
			return
		case <-h.AmqpConnDone():
			log.Error("amqp conn done.")
			return
		case delivery, ok := <-consumer.Delivery():
			if !ok {
				log.Error("not ok")
				return
			}

			handle(h.GetDBConn(), delivery, false)
		}
	}
}

func handle(db *gorm.DB, delivery amqp.Delivery, ackMsg bool) {
	defer delivery.Ack(false)
	var (
		msg = &Message{}
		err error
		now = time.Now()
	)
	if err = json.Unmarshal(delivery.Body, &msg); err != nil {
		log.Error(err)
		return
	}
	var queue = &models.Queue{
		UniqueId: msg.UniqueId,
	}
	if err = db.Model(&models.Queue{}).Where("unique_id", msg.UniqueId).First(&queue).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			if ackMsg {
				db.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "unique_id"}},
					DoUpdates: clause.AssignmentColumns([]string{"acked_at"}),
				}).Create(&models.Queue{
					UniqueId: msg.UniqueId,
					AckedAt:  &now,
				})
			} else {
				db.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "unique_id"}},
					DoUpdates: clause.AssignmentColumns([]string{"retry_times", "confirmed_at", "data", "queue_name", "ref"}),
				}).Create(&models.Queue{
					UniqueId:    msg.UniqueId,
					RetryTimes:  msg.RetryTimes,
					ConfirmedAt: &now,
					Data:        msg.Data,
					QueueName:   msg.QueueName,
					Ref:         msg.Ref,
				})
			}
		} else {
			log.Error(err)
		}
		return
	}

	if queue.NAcked() {
		log.Warn("queue status", queue.NAckedAt)
		return
	}

	if ackMsg {
		db.Model(&models.Queue{ID: queue.ID}).Updates(&models.Queue{AckedAt: &now})
	} else {
		db.Model(&models.Queue{ID: queue.ID}).Updates(&models.Queue{
			RetryTimes:  msg.RetryTimes,
			ConfirmedAt: &now,
			Data:        msg.Data,
			QueueName:   msg.QueueName,
			Ref:         msg.Ref,
		})
	}
}

func (h *Hub) ConsumeAckQueue() {
	var (
		err      error
		consumer ConsumerInterface
	)

	defer func() {
		log.Error("ConsumeAckQueue EXIT")
	}()

	if consumer, err = h.GetAckQueueConsumer(); err != nil {
		log.Error("err GetAckQueueConsumer()", err)
		return
	}
	defer consumer.Close()

	for {
		select {
		case <-consumer.Done():
			return
		case <-h.Done():
			log.Error("hub done ConsumeAckQueue exit.")
			return
		case <-h.AmqpConnDone():
			log.Error("amqp conn done.")
			return
		case delivery, ok := <-consumer.Delivery():
			if !ok {
				log.Error("not ok")
				return
			}

			handle(h.GetDBConn(), delivery, true)
		}
	}
}

func (h *Hub) GetConfirmConsumer() (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	if consumer, err = h.ConsumerManager().GetConsumer(ConfirmQueueName, amqp.ExchangeDirect); err != nil {
		return nil, err
	}

	return consumer, nil
}

func (h *Hub) GetConfirmProducer() (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if producer, err = h.ProducerManager().GetProducer(ConfirmQueueName, amqp.ExchangeDirect); err != nil {
		return nil, err
	}

	return producer, nil
}

func (h *Hub) GetAckQueueConsumer() (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	if consumer, err = h.ConsumerManager().GetConsumer(AckQueueName, amqp.ExchangeDirect); err != nil {
		return nil, err
	}

	return consumer, nil
}

func (h *Hub) GetAckQueueProducer() (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if producer, err = h.ProducerManager().GetProducer(AckQueueName, amqp.ExchangeDirect); err != nil {
		return nil, err
	}

	return producer, nil
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

	if h.IsClosed() {
		return nil, ServerUnavailable
	}

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

	if h.IsClosed() {
		return nil, ServerUnavailable
	}

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

func Ack(db *gorm.DB, uniqueId string) error {
	var queue = &models.Queue{UniqueId: uniqueId}
	now := time.Now()
	if err := db.Find(queue).Error; err != nil {
		return err
	}

	if queue.NAcked() {
		return errors.New("already nacked")
	}

	if queue.Acked() {
		return nil
	}

	queue.AckedAt = &now
	db.Updates(queue)

	return nil
}

func Nack(db *gorm.DB, uniqueId string) error {
	var queue = &models.Queue{
		UniqueId: uniqueId,
	}
	now := time.Now()
	if err := db.Find(queue).Error; err != nil {
		return err
	}

	if queue.Acked() {
		return errors.New("already acked")
	}

	if queue.NAcked() {
		return nil
	}
	queue.NAckedAt = &now
	db.Updates(queue)

	return nil
}

func DelayPublish(db *gorm.DB, queueName string, msg Message) (*models.DelayQueue, error) {
	startTime := time.Now().Add(time.Duration(msg.DelaySeconds) * time.Second)

	delayQueue := &models.DelayQueue{
		UniqueId:     xid.New().String(),
		RunAfter:     &startTime,
		DelaySeconds: msg.DelaySeconds,
		Data:         msg.Data,
		QueueName:    queueName,
	}

	if err := db.Create(delayQueue).Error; err != nil {
		return nil, err
	}

	log.Debug(delayQueue)
	return delayQueue, nil
}
