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
var _ BackgroundJobWorker = (*Hub)(nil)

type atomicBool int32

func (b *atomicBool) isSet() bool { return atomic.LoadInt32((*int32)(b)) != 0 }
func (b *atomicBool) setTrue()    { atomic.StoreInt32((*int32)(b), 1) }
func (b *atomicBool) setFalse()   { atomic.StoreInt32((*int32)(b), 0) }

type BackgroundJobWorker interface {
	Run()

	GetDelayPublishProducer() (ProducerInterface, error)
	GetDelayPublishConsumer() (ConsumerInterface, error)
	ConsumeDelayPublishQueue()

	GetConfirmProducer() (ProducerInterface, error)
	GetConfirmConsumer() (ConsumerInterface, error)
	ConsumeConfirmQueue()

	GetAckQueueProducer() (ProducerInterface, error)
	GetAckQueueConsumer() (ConsumerInterface, error)
	ConsumeAckQueue()
}

type Interface interface {
	Ack(string) error
	Nack(string) error

	DelayPublish(queueName, kind string, msg Message, delaySeconds uint) error

	ConsumerManager() ConsumerManagerInterface
	ProducerManager() ProducerManagerInterface

	NewDurableNotAutoDeleteProducer(queueName, kind string, opts ...Option) (ProducerInterface, error)
	NewDurableNotAutoDeleteConsumer(queueName, kind string, opts ...Option) (ConsumerInterface, error)

	RemoveProducer(p ProducerInterface)
	RemoveConsumer(c ConsumerInterface)

	CloseAllConsumer()
	CloseAllProducer()

	ReBalance(queueName, kind, exchange string)

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
	rb  *Rebalancer

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
	h.rb = NewRebalancer(h)

	h.listenAmqpConnDone()

	return h
}

func (h *Hub) ReBalance(queueName, kind, exchange string) {
	h.rb.ReBalance(queueName, kind, exchange)
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
	if err = h.GetDBConn().Unscoped().Model(&models.Queue{}).Where("unique_id = ?", uniqueId).First(&queue).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			if err = h.GetDBConn().Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "unique_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"nacked_at", "run_after", "status"}),
			}).Create(&models.Queue{
				UniqueId: uniqueId,
				NackedAt: &now,
				Status:   models.StatusNacked,
				RunAfter: &now,
			}).Error; err != nil {
				log.Error(err)
				return err
			}
		} else {
			log.Error(err)
		}
	}

	if queue.Deleted() || queue.Nackd() {
		return nil
	}

	if queue.Acked() {
		return ErrorAlreadyAcked
	}

	if err = h.GetDBConn().Model(&models.Queue{}).Where("id = ?", queue.ID).Updates(&models.Queue{NackedAt: &now, RunAfter: &now, Status: models.StatusNacked}).Error; err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func (h *Hub) ConsumeDelayPublishQueue() {
	for i := 0; i < h.Config().BackConsumerGoroutineNum; i++ {
		go h.consumeDelayPublishQueue()
	}
	log.Debug("back consume confirm queue started.")
}

func (h *Hub) ConsumeConfirmQueue() {
	for i := 0; i < h.Config().BackConsumerGoroutineNum; i++ {
		go h.consumeConfirmQueue()
	}
	log.Debug("back consume confirm queue started.")
}

func (h *Hub) consumeConfirmQueue() {
	var (
		err      error
		consumer ConsumerInterface
	)

	defer func() {
		log.Debug("ConsumeConfirmQueue EXIT")
	}()

	if consumer, err = h.GetConfirmConsumer(); err != nil {
		log.Error("err ConsumeConfirmQueue()", err)
		return
	}
	defer consumer.Close()

	for {
		select {
		case <-consumer.ChannelDone():
			return
		case <-h.Done():
			log.Debug("hub done ConsumeConfirmQueue exit.")
			return
		case delivery, ok := <-consumer.Delivery():
			if !ok {
				log.Debug("not ok")
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
	log.Debug("back consume ack queue started.")
}

func (h *Hub) consumeAckQueue() {
	var (
		err      error
		consumer ConsumerInterface
	)

	defer func() {
		log.Debug("ConsumeAckQueue EXIT")
	}()

	if consumer, err = h.GetAckQueueConsumer(); err != nil {
		log.Error("err GetAckQueueConsumer()", err)
		return
	}

	defer consumer.Close()

	for {
		select {
		case <-consumer.ChannelDone():
			return
		case <-h.Done():
			log.Debug("hub done ConsumeAckQueue exit.")
			return
		case delivery, ok := <-consumer.Delivery():
			if !ok {
				log.Debug("not ok")
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

	if consumer, err = h.ConsumerManager().GetDurableNotAutoDeleteConsumer(DelayQueueName, amqp.ExchangeDirect, DefaultExchange); err != nil {
		return nil, err
	}

	return consumer, nil
}

func (h *Hub) GetConfirmConsumer() (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	if consumer, err = h.ConsumerManager().GetDurableNotAutoDeleteConsumer(ConfirmQueueName, amqp.ExchangeDirect, DefaultExchange); err != nil {
		return nil, err
	}

	return consumer, nil
}

func (h *Hub) Run() {
	go h.ConsumeConfirmQueue()
	go h.ConsumeAckQueue()
	go h.ConsumeDelayPublishQueue()
}

func (h *Hub) GetDelayPublishProducer() (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if producer, err = h.ProducerManager().GetDurableNotAutoDeleteProducer(DelayQueueName, amqp.ExchangeDirect, DefaultExchange); err != nil {
		return nil, err
	}

	return producer, nil
}

func (h *Hub) GetConfirmProducer() (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if producer, err = h.ProducerManager().GetDurableNotAutoDeleteProducer(ConfirmQueueName, amqp.ExchangeDirect, DefaultExchange); err != nil {
		return nil, err
	}

	return producer, nil
}

func (h *Hub) GetAckQueueConsumer() (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	if consumer, err = h.ConsumerManager().GetDurableNotAutoDeleteConsumer(AckQueueName, amqp.ExchangeDirect, DefaultExchange); err != nil {
		return nil, err
	}

	return consumer, nil
}

func (h *Hub) GetAckQueueProducer() (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if producer, err = h.ProducerManager().GetDurableNotAutoDeleteProducer(AckQueueName, amqp.ExchangeDirect, DefaultExchange); err != nil {
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

func (h *Hub) NewDurableNotAutoDeleteProducer(queueName, kind string, opts ...Option) (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if h.IsClosed() {
		log.Debug("hub.NewDurableNotAutoDeleteProducer but hub closed.")
		return nil, ErrorServerUnavailable
	}

	if producer, err = h.ProducerManager().GetDurableNotAutoDeleteProducer(queueName, kind, DefaultExchange, opts...); err != nil {
		log.Debugf("hub.NewDurableNotAutoDeleteProducer GetDurableNotAutoDeleteProducer err %v.", err)
		return nil, err
	}

	return producer, nil
}

func (h *Hub) NewDurableNotAutoDeleteConsumer(queueName, kind string, opts ...Option) (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	if h.IsClosed() {
		return nil, ErrorServerUnavailable
	}

	if consumer, err = h.ConsumerManager().GetDurableNotAutoDeleteConsumer(queueName, kind, DefaultExchange, opts...); err != nil {
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

func (h *Hub) DelayPublish(queueName, kind string, msg Message, delaySeconds uint) error {
	var (
		producer ProducerInterface
		err      error
	)
	msg.QueueName = queueName
	msg.Kind = kind
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
		log.Debug("ConsumeDelayPublishQueue EXIT")
	}()

	if consumer, err = h.GetDelayPublishConsumer(); err != nil {
		log.Error("err ConsumeDelayPublishQueue()", err)
		return
	}
	defer consumer.Close()

	for {
		select {
		case <-consumer.ChannelDone():
			return
		case <-h.Done():
			log.Debug("hub done ConsumeConfirmQueue exit.")
			return
		case delivery, ok := <-consumer.Delivery():
			if !ok {
				log.Debug("not ok")
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

			if err = h.GetDBConn().Create(&models.DelayQueue{
				Kind:         msg.Kind,
				RetryTimes:   msg.RetryTimes,
				UniqueId:     msg.UniqueId,
				Data:         msg.Data,
				QueueName:    msg.QueueName,
				RunAfter:     &runAfter,
				DelaySeconds: msg.DelaySeconds,
				Ref:          msg.Ref,
			}).Error; err != nil {
				delivery.Nack(false, true)
				return
			}
			delivery.Ack(false)
		}
	}
}

func handle(db *gorm.DB, delivery amqp.Delivery, ackMsg bool) {
	var (
		msg = &Message{}
		err error
		now = time.Now()
	)

	defer func() {
		if err != nil {
			log.Error("出现了不应该出现的异常", err)
			delivery.Nack(false, true)
		} else {
			delivery.Ack(false)
		}
	}()

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
				if err = db.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "unique_id"}},
					DoUpdates: clause.AssignmentColumns([]string{"acked_at", "status"}),
				}).Create(&models.Queue{
					UniqueId: msg.UniqueId,
					AckedAt:  &msg.AckedAt,
					Status:   models.StatusAcked,
				}).Error; err != nil {
					log.Error(err)
				}
			} else {
				if err = db.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "unique_id"}},
					DoUpdates: clause.AssignmentColumns([]string{"kind", "run_after", "retry_times", "confirmed_at", "data", "queue_name", "ref", "is_confirmed"}),
				}).Create(&models.Queue{
					Kind:        msg.Kind,
					UniqueId:    msg.UniqueId,
					RetryTimes:  msg.RetryTimes,
					ConfirmedAt: &now,
					Data:        msg.Data,
					QueueName:   msg.QueueName,
					RunAfter:    msg.RunAfter,
					Ref:         msg.Ref,
					IsConfirmed: true,
				}).Error; err != nil {
					log.Error(err)
				}
			}
		} else {
			log.Error(err)
		}
		return
	}

	if queue.Deleted() {
		log.Warnf("queue %s already deleted ", queue.UniqueId)
		return
	}

	if queue.Nackd() {
		if !queue.Confirmed() {
			if err = db.Model(&models.Queue{ID: queue.ID}).Updates(&models.Queue{
				Kind:        msg.Kind,
				RetryTimes:  msg.RetryTimes,
				ConfirmedAt: &now,
				Data:        msg.Data,
				QueueName:   msg.QueueName,
				RunAfter:    msg.RunAfter,
				Ref:         msg.Ref,
				IsConfirmed: true,
			}).Error; err != nil {
				log.Error(err)
			}
		}
		log.Debug("queue status ", queue.NackedAt)
		return
	}

	if ackMsg && !queue.Acked() {
		if err = db.Model(&models.Queue{ID: queue.ID}).Updates(&models.Queue{AckedAt: &msg.AckedAt, Status: models.StatusAcked}).Error; err != nil {
			log.Error(err)
		}
	} else if !queue.Confirmed() {
		if err = db.Model(&models.Queue{ID: queue.ID}).Updates(&models.Queue{
			Kind:        msg.Kind,
			RetryTimes:  msg.RetryTimes,
			ConfirmedAt: &now,
			Data:        msg.Data,
			QueueName:   msg.QueueName,
			RunAfter:    msg.RunAfter,
			Ref:         msg.Ref,
			IsConfirmed: true,
		}).Error; err != nil {
			log.Error(err)
		}
	}
}

func (h *Hub) listenAmqpConnDone() {
	go func() {
		defer log.Warn("listenAmqpConnDone EXIT.")
		select {
		case <-h.ctx.Done():
			return
		case <-h.notifyConnClose:
			h.cancel()
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
			h.pm = NewProducerManager(h)
			h.cm = NewConsumerManager(h)
			h.rb = NewRebalancer(h)
			if h.Config().BackgroundConsumerEnabled {
				h.Run()
			}
			h.listenAmqpConnDone()
		}
	}()
}
