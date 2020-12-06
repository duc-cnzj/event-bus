package hub

import (
	"context"
	"database/sql"
	json "github.com/json-iterator/go"
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

type Interface interface {
	Ack(string) error
	Nack(string) error

	DelayPublishQueue(models.DelayQueue) error

	ConsumerManager() ConsumerManagerInterface
	ProducerManager() ProducerManagerInterface

	NewProducer(queueName, kind, exchange string, durableExchange, exchangeAutoDelete, durableQueue, queueAutoDelete bool) (ProducerInterface, error)
	NewConsumer(queueName, kind, exchange string, durableExchange, exchangeAutoDelete, durableQueue, queueAutoDelete, reBalance, autoAck bool) (ConsumerInterface, error)

	//For Direct
	NewDurableNotAutoDeleteDirectProducer(queueName string) (ProducerInterface, error)
	NewDurableNotAutoDeleteDirectConsumer(queueName string, reBalance bool) (ConsumerInterface, error)

	//For Pubsub
	NewDurableNotAutoDeletePubsubProducer(queueName string) (ProducerInterface, error)
	NewDurableNotAutoDeletePubsubConsumer(queueName, exchange string) (ConsumerInterface, error)

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

func (h *Hub) NewDurableNotAutoDeletePubsubProducer(exchange string) (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if h.IsClosed() {
		log.Debug("hub.NewDurableNotAutoDeleteDirectProducer but hub closed.")
		return nil, ErrorServerUnavailable
	}

	if producer, err = h.NewProducer("", amqp.ExchangeFanout, exchange, true, false, true, false); err != nil {
		log.Debugf("hub.NewDurableNotAutoDeleteDirectProducer GetDurableNotAutoDeleteProducer err %v.", err)
		return nil, err
	}

	return producer, nil
}

func (h *Hub) NewDurableNotAutoDeletePubsubConsumer(queueName, exchange string) (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	if h.IsClosed() {
		return nil, ErrorServerUnavailable
	}

	if consumer, err = h.NewConsumer(queueName, amqp.ExchangeFanout, exchange, true, false, true, false, true, false); err != nil {
		return nil, err
	}

	return consumer, nil
}

func (h *Hub) NewProducer(queueName, kind, exchange string, durableExchange, exchangeAutoDelete, durableQueue, queueAutoDelete bool) (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	opts := []Option{
		WithExchangeDurable(true),
		WithExchangeDurable(durableExchange),
		WithExchangeAutoDelete(exchangeAutoDelete),
		WithQueueDurable(durableQueue),
		WithQueueAutoDelete(queueAutoDelete),
	}

	if h.IsClosed() {
		return nil, ErrorServerUnavailable
	}

	if producer, err = h.ProducerManager().GetProducer(queueName, kind, exchange, opts...); err != nil {
		return nil, err
	}

	return producer, nil
}

func (h *Hub) ReBalance(queueName, kind, exchange string) {
	log.Debugf("重平衡心跳检查 queue %s kind %s exchange %s", queueName, kind, exchange)
	h.rb.ReBalance(queueName, kind, exchange)
}

func (h *Hub) Ack(uniqueId string) error {
	var (
		err        error
		producer   ProducerInterface
		ackJsonMsg []byte
	)

	if producer, err = h.GetAckQueueProducer(); err != nil {
		return err
	}

	if ackJsonMsg, err = json.Marshal(NewAckMessage(uniqueId, time.Now())); err != nil {
		return err
	}

	if err = producer.Publish(NewMessage(string(ackJsonMsg))); err != nil {
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
	runAfter := now.Add(time.Duration(h.Config().NackdJobNextRunDelaySeconds) * time.Second)

	if err = h.GetDBConn().Unscoped().Model(&models.Queue{}).Where("unique_id = ?", uniqueId).First(&queue).Error; err != nil {
		if err == gorm.ErrRecordNotFound {

			if err = h.GetDBConn().Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "unique_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"nacked_at", "run_after", "status"}),
			}).Create(&models.Queue{
				UniqueId: uniqueId,
				NackedAt: &now,
				Status:   models.StatusNacked,
				RunAfter: &runAfter,
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

	if err = h.GetDBConn().Model(&models.Queue{}).Where("id = ?", queue.ID).Updates(&models.Queue{NackedAt: &now, RunAfter: &runAfter, Status: models.StatusNacked}).Error; err != nil {
		log.Error(err)
		return err
	}

	return nil
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

func (h *Hub) NewDurableNotAutoDeleteDirectProducer(queueName string) (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if h.IsClosed() {
		log.Debug("hub.NewDurableNotAutoDeleteDirectProducer but hub closed.")
		return nil, ErrorServerUnavailable
	}

	if producer, err = h.NewProducer(queueName, amqp.ExchangeDirect, DefaultExchange, true, false, true, false); err != nil {
		log.Debugf("hub.NewDurableNotAutoDeleteDirectProducer GetDurableNotAutoDeleteProducer err %v.", err)
		return nil, err
	}

	return producer, nil
}

func (h *Hub) NewConsumer(queueName, kind, exchange string, durableExchange, exchangeAutoDelete, durableQueue, queueAutoDelete, reBalance, autoAck bool) (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	opts := []Option{
		WithExchangeDurable(durableExchange),
		WithQueueDurable(durableQueue),
		WithQueueAutoDelete(queueAutoDelete),
		WithExchangeAutoDelete(exchangeAutoDelete),
		WithConsumerReBalance(reBalance),
		WithConsumerAck(!autoAck),
	}

	if h.IsClosed() {
		return nil, ErrorServerUnavailable
	}

	if consumer, err = h.ConsumerManager().GetConsumer(queueName, kind, exchange, opts...); err != nil {
		return nil, err
	}

	return consumer, nil
}

func (h *Hub) NewDurableNotAutoDeleteDirectConsumer(queueName string, reBalance bool) (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	if h.IsClosed() {
		return nil, ErrorServerUnavailable
	}

	if consumer, err = h.NewConsumer(queueName, amqp.ExchangeDirect, DefaultExchange, true, false, true, false, reBalance, false); err != nil {
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

func (h *Hub) DelayPublishQueue(dq models.DelayQueue) error {
	var (
		producer ProducerInterface
		err      error
	)
	if producer, err = h.GetDelayPublishProducer(); err != nil {
		return err
	}
	data, _ := json.Marshal(&dq)

	if err = producer.Publish(NewMessage(string(data))); err != nil {
		return err
	}

	return nil
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
