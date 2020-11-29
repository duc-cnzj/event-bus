package hub

import (
	"context"
	"database/sql"
	json "github.com/json-iterator/go"
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

var _ Interface = (*Hub)(nil)

type atomicBool int32

func (b *atomicBool) isSet() bool { return atomic.LoadInt32((*int32)(b)) != 0 }
func (b *atomicBool) setTrue()    { atomic.StoreInt32((*int32)(b), 1) }
func (b *atomicBool) setFalse()   { atomic.StoreInt32((*int32)(b), 0) }

type Interface interface {
	Ack(string) error
	Nack(string) error

	DelayPublish(string, Message, uint) error

	RunBackgroundJobs()

	GetDelayPublishProducer() (ProducerInterface, error)
	GetDelayPublishConsumer() (ConsumerInterface, error)
	ConsumeDelayPublishQueue()

	GetConfirmProducer() (ProducerInterface, error)
	GetConfirmConsumer() (ConsumerInterface, error)
	ConsumeConfirmQueue()

	GetAckQueueProducer() (ProducerInterface, error)
	GetAckQueueConsumer() (ConsumerInterface, error)
	ConsumeAckQueue()

	ConsumerManager() ConsumerManagerInterface
	ProducerManager() ProducerManagerInterface

	NewProducer(queueName, kind string) (ProducerInterface, error)
	NewConsumer(queueName, kind string) (ConsumerInterface, error)

	RemoveProducer(p ProducerInterface)
	RemoveConsumer(c ConsumerInterface)

	CloseAllConsumer()
	CloseAllProducer()

	// todo remove
	GetAmqpConn() (*amqp.Connection, error)

	GetDBConn() *gorm.DB

	IsClosed() bool
	Close()

	Done() <-chan struct{}

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

	h.listenAmqpConnDone()

	go func() {
		for {
			select {
			case <-h.notifyConnClose:
				if h.IsClosed() {
					return
				}
				log.Error("amqp 连接断开")
				h.amqpConn.Close()
				log.Error("amqp 开始重连")
				h.amqpConn = conn2.ReConnect(h.Config().AmqpUrl)
				h.notifyConnClose = h.amqpConn.NotifyClose(make(chan *amqp.Error))
				cancel, cancelFunc := context.WithCancel(context.Background())
				h.ctx = cancel
				h.cancel = cancelFunc
				h.listenAmqpConnDone()
				h.pm = NewProducerManager(h)
				h.cm = NewConsumerManager(h)
				if h.Config().BackgroundConsumerEnabled {
					h.RunBackgroundJobs()
				}
			}
		}
	}()

	return h
}

func (h *Hub) Ack(uniqueId string) error {
	var (
		err      error
		producer ProducerInterface
	)

	if producer, err = h.GetAckQueueProducer(); err != nil {
		return err
	}

	if err = producer.Publish(Message{UniqueId: uniqueId, AckedAt: time.Now()}); err != nil {
		return err
	}

	return nil
}

func (h *Hub) Nack(uniqueId string) error {
	var (
		err   error
		queue models.Queue
	)
	now := time.Now()
	if err = h.GetDBConn().Unscoped().Model(&models.Queue{}).Where("unique_id", uniqueId).First(&queue).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			h.GetDBConn().Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "unique_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"nacked_at", "run_after"}),
			}).Create(&models.Queue{
				UniqueId: uniqueId,
				NAckedAt: &now,
				RunAfter: &now,
			})
		}
	}

	if queue.Deleted() || queue.Nackd() {
		return nil
	}

	if queue.Acked() {
		return ErrorAlreadyAcked
	}

	h.GetDBConn().Model(&models.Queue{ID: queue.ID}).Updates(&models.Queue{NAckedAt: &now, RunAfter: &now})

	return nil
}

func (h *Hub) ConsumeDelayPublishQueue() {
	for i := 0; i < h.Config().BackConsumerGoroutineNum; i++ {
		go h.consumeDelayPublishQueue()
	}
	log.Infof("back consume confirm queue started.")
}

func (h *Hub) ConsumeConfirmQueue() {
	for i := 0; i < h.Config().BackConsumerGoroutineNum; i++ {
		go h.consumeConfirmQueue()
	}
	log.Infof("back consume confirm queue started.")
}

func (h *Hub) consumeConfirmQueue() {
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

	for {
		select {
		case <-consumer.ChannelDone():
			return
		case <-h.Done():
			log.Error("hub done ConsumeConfirmQueue exit.")
			return
		case delivery, ok := <-consumer.Delivery():
			if !ok {
				log.Error("not ok")
				delivery.Nack(false, true)
				return
			}

			handle(h.GetDBConn(), delivery, false)
		}
	}
}

func (h *Hub) ConsumeAckQueue() {
	for i := 0; i < h.Config().BackConsumerGoroutineNum; i++ {
		go h.consumeAckQueue()
	}
	log.Infof("back consume ack queue started.")
}

func (h *Hub) consumeAckQueue() {
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

	for {
		select {
		case <-consumer.ChannelDone():
			return
		case <-h.Done():
			log.Error("hub done ConsumeAckQueue exit.")
			return
		case delivery, ok := <-consumer.Delivery():
			if !ok {
				log.Error("not ok")
				delivery.Nack(false, true)
				return
			}

			handle(h.GetDBConn(), delivery, true)
		}
	}
}

func (h *Hub) GetDelayPublishConsumer() (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	if consumer, err = h.ConsumerManager().GetConsumer(DelayQueueName, amqp.ExchangeDirect); err != nil {
		return nil, err
	}

	return consumer, nil
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

func (h *Hub) RunBackgroundJobs() {
	go h.ConsumeConfirmQueue()
	go h.ConsumeAckQueue()
	go h.ConsumeDelayPublishQueue()
}

func (h *Hub) GetDelayPublishProducer() (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if producer, err = h.ProducerManager().GetProducer(DelayQueueName, amqp.ExchangeDirect); err != nil {
		return nil, err
	}

	return producer, nil
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
		log.Debug("hub.NewProducer but hub closed.")
		return nil, ErrorServerUnavailable
	}

	if producer, err = h.ProducerManager().GetProducer(queueName, kind); err != nil {
		log.Debugf("hub.NewProducer GetProducer err %v.", err)
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
		return nil, ErrorServerUnavailable
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

func (h *Hub) listenAmqpConnDone() {
	go func() {
		defer log.Warn("listenAmqpConnDone EXIT.")
		select {
		case <-h.ctx.Done():
			return
		case <-h.notifyConnClose:
			h.cancel()
			log.Warn("amqp done cancel().")
		}
	}()
}

func (h *Hub) Done() <-chan struct{} {
	return h.ctx.Done()
}

func (h *Hub) GetAmqpConn() (*amqp.Connection, error) {
	if !h.amqpConn.IsClosed() {
		return h.amqpConn, nil
	}

	return nil, ErrorAmqpConnClosed
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
		h.CloseAllConsumer()

		log.Info("hub amqp conn closing.")
		timeout, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
		go func() {
			if err = h.amqpConn.Close(); err != nil {
				log.Error(err)
			}
			cancelFunc()
			log.Println("amqp 正常退出")
		}()
		log.Info("wait amqp conn done....")
		<-timeout.Done()
		log.Info("hub amqp conn closed.")
	}

	log.Info("db closing.")
	if db, err = h.db.DB(); err != nil {
		log.Error(err)
	}
	if err = db.Close(); err != nil {
		log.Error(err)
	}
	log.Info("db closed .")

	log.Info("hub closed.")
}

func (h *Hub) Config() *config.Config {
	return h.cfg
}

func (h *Hub) DelayPublish(queueName string, msg Message, delaySeconds uint) error {
	var (
		producer ProducerInterface
		err      error
	)
	msg.QueueName = queueName
	msg.DelaySeconds = delaySeconds
	runAfter := time.Now().Add(time.Duration(delaySeconds) * time.Second)
	msg.RunAfter = &runAfter

	if msg.UniqueId == "" {
		msg.UniqueId = xid.New().String()
	}

	if producer, err = h.GetDelayPublishProducer(); err != nil {
		return err
	}

	if err = producer.Publish(msg); err != nil {
		return err
	}

	return nil
}

func (h *Hub) consumeDelayPublishQueue() {
	var (
		err      error
		consumer ConsumerInterface
	)

	defer func() {
		log.Error("ConsumeDelayPublishQueue EXIT")
	}()

	if consumer, err = h.GetDelayPublishConsumer(); err != nil {
		log.Error("err ConsumeDelayPublishQueue()", err)
		return
	}

	for {
		select {
		case <-consumer.ChannelDone():
			return
		case <-h.Done():
			log.Error("hub done ConsumeConfirmQueue exit.")
			return
		case delivery, ok := <-consumer.Delivery():
			if !ok {
				log.Error("not ok")
				delivery.Nack(false, true)
				return
			}

			var (
				msg = &Message{}
				err error
			)
			if err = json.Unmarshal(delivery.Body, &msg); err != nil {
				log.Error(err)
				delivery.Nack(false, true)
				return
			}
			runAfter := time.Now().Add(time.Duration(msg.DelaySeconds) * time.Second)

			h.GetDBConn().Create(&models.DelayQueue{
				UniqueId:     msg.UniqueId,
				Data:         msg.Data,
				QueueName:    msg.QueueName,
				RunAfter:     &runAfter,
				DelaySeconds: msg.DelaySeconds,
				Ref:          msg.Ref,
			})
			delivery.Ack(false)
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
	if err = db.Unscoped().Model(&models.Queue{}).Where("unique_id", msg.UniqueId).First(&queue).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			if ackMsg {
				db.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "unique_id"}},
					DoUpdates: clause.AssignmentColumns([]string{"acked_at"}),
				}).Create(&models.Queue{
					UniqueId: msg.UniqueId,
					AckedAt:  &msg.AckedAt,
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
					RunAfter:    msg.RunAfter,
					Ref:         msg.Ref,
				})
			}
		} else {
			log.Error(err)
		}
		return
	}

	if queue.Deleted() {
		log.Warn("queue already deleted ", queue.UniqueId)
		return
	}

	if queue.Nackd() {
		if !ackMsg {
			db.Model(&models.Queue{ID: queue.ID}).Updates(&models.Queue{
				RetryTimes:  msg.RetryTimes,
				ConfirmedAt: &now,
				Data:        msg.Data,
				QueueName:   msg.QueueName,
				RunAfter:    msg.RunAfter,
				Ref:         msg.Ref,
			})
		}
		log.Warn("queue status", queue.NAckedAt)
		return
	}

	if ackMsg {
		db.Model(&models.Queue{ID: queue.ID}).Updates(&models.Queue{AckedAt: &msg.AckedAt})
	} else {
		db.Model(&models.Queue{ID: queue.ID}).Updates(&models.Queue{
			RetryTimes:  msg.RetryTimes,
			ConfirmedAt: &now,
			Data:        msg.Data,
			QueueName:   msg.QueueName,
			RunAfter:    msg.RunAfter,
			Ref:         msg.Ref,
		})
	}
}
