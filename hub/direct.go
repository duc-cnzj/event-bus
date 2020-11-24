package hub

import (
	"context"
	"encoding/json"
	"errors"
	"mq/models"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ServerUnavailable = status.Errorf(codes.Unavailable, "server unavailable")

type DirectProducer struct {
	*ProducerBase
}

func NewDirectProducer(queueName string, hub Interface) ProducerBuilder {
	d := &DirectProducer{ProducerBase: &ProducerBase{
		pm:        hub.ProducerManager(),
		queueName: queueName,
		kind:      amqp.ExchangeDirect,
		hub:       hub,
		exchange:  DefaultExchange,
	}}

	return d
}

func (d *DirectProducer) Publish(message Message) error {
	marshal, err := json.Marshal(&message)
	if err != nil {
		return err
	}

	return d.channel.Publish(
		d.exchange,
		d.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        marshal,
		},
	)
}

func (d *DirectProducer) Done() chan *amqp.Error {
	return d.closeChan
}

func (d *DirectProducer) PrepareConn() error {
	defer func(t time.Time) { log.Warn("DirectProducer PrepareConn", time.Since(t)) }(time.Now())

	var (
		conn *amqp.Connection
		err  error
	)

	if conn, err = d.hub.GetAmqpConn(); err != nil {
		return err
	}

	d.conn = conn

	return nil
}

func (d *DirectProducer) GetConn() *amqp.Connection {
	return d.conn
}

func (d *DirectProducer) GetChannel() *amqp.Channel {
	return d.channel
}

func (d *DirectProducer) GetQueueName() string {
	return d.queue.Name
}

func (d *DirectProducer) GetKind() string {
	return d.kind
}

func (d *DirectProducer) PrepareChannel() error {
	defer func(t time.Time) { log.Warn("DirectProducer prepareChannel", time.Since(t)) }(time.Now())

	var (
		err error
	)

	if d.channel, err = d.conn.Channel(); err != nil {
		return err
	}

	return nil
}

func (d *DirectProducer) PrepareExchange() error {
	defer func(t time.Time) { log.Warn("DirectProducer prepareExchange", time.Since(t)) }(time.Now())

	var (
		err error
	)
	if err = d.channel.ExchangeDeclare(
		d.exchange,
		amqp.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}
	return nil
}

func (d *DirectProducer) PrepareQueueDeclare() error {
	defer func(t time.Time) {
		log.Warnf("DirectProducer prepareQueueDeclare queueName %s %v", d.queueName, time.Since(t))
	}(time.Now())

	var (
		err   error
		queue amqp.Queue
	)

	if queue, err = d.channel.QueueDeclare(
		d.queueName,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		log.Error(err)
		return err
	}

	d.queue = queue

	return nil
}

func (d *DirectProducer) PrepareQueueBind() error {
	defer func(t time.Time) { log.Warn("DirectProducer prepareQueueBind", time.Since(t)) }(time.Now())

	var err error
	if err = d.channel.QueueBind(d.queue.Name, d.queue.Name, DefaultExchange, false, nil); err != nil {
		return err
	}
	return nil
}

func (d *DirectProducer) Build() (ProducerInterface, error) {
	var (
		err error
	)

	if err = d.PrepareConn(); err != nil {
		log.Info("NewProducer PrepareConn", err)
		return nil, err
	}

	if err = d.PrepareChannel(); err != nil {
		log.Info("NewProducer prepareChannel", err)
		return nil, err
	}

	if err = d.PrepareExchange(); err != nil {
		log.Info("NewProducer prepareExchange", err)
		return nil, err
	}

	if err = d.PrepareQueueDeclare(); err != nil {
		log.Info("NewProducer prepareQueueDeclare", err)

		return nil, err
	}

	if err = d.PrepareQueueBind(); err != nil {
		log.Info("NewProducer prepareQueueBind", err)

		return nil, err
	}

	d.closeChan = d.channel.NotifyClose(make(chan *amqp.Error))

	go func() {
		defer func() {
			d.Close()
			log.Warnf("PRODUCER EXIT %s", d.GetQueueName())
		}()

		select {
		case <-d.Done():
			//log.Info("producer listener return when producer channel closed")
		case <-d.hub.Done():
			//log.Info("producer listener return hub ctx done")
		case <-d.hub.AmqpConnDone():
			//log.Info("producer listener return when hub.notifyConnClose")
		}
	}()

	return d, nil
}

func (d *DirectProducer) RemoveSelf() {
	d.pm.RemoveProducer(d)
}

func (d *DirectProducer) Close() {
	if d.closed.isSet() {
		log.Infof("producer %s already closed.", d.GetQueueName())
		return
	}
	d.closed.setTrue()
	select {
	case <-d.Done():
		log.Info("producer exit when d.Done()")
	case <-d.hub.AmqpConnDone():
		if err := d.channel.Close(); err != nil {
			log.Errorf("Close channel err %v %s", err, d.GetQueueName())
		}
	default:
		if err := d.channel.Close(); err != nil {
			log.Errorf("Close channel err %v %s", err, d.GetQueueName())
		}
	}
	d.RemoveSelf()
	log.Infof("####### producer closed %s #######", d.GetQueueName())
}

type DirectConsumer struct {
	*ConsumerBase
}

func (d *DirectConsumer) Ack(id uint64) error {
	if err := Ack(d.hub.GetDBConn(), id); err != nil {
		return err
	}
	return nil
}

func (d *DirectConsumer) Nack(id uint64) error {
	return Nack(d.hub.GetDBConn(), id)
}

func NewDirectConsumer(queueName string, hub Interface) ConsumerBuilder {
	return &DirectConsumer{ConsumerBase: &ConsumerBase{
		cm:        hub.ConsumerManager(),
		queueName: queueName,
		kind:      amqp.ExchangeDirect,
		hub:       hub,
		exchange:  DefaultExchange,
	}}
}

func (d *DirectConsumer) GetConn() *amqp.Connection {
	return d.conn
}

func (d *DirectConsumer) GetChannel() *amqp.Channel {
	return d.channel
}

func (d *DirectConsumer) GetQueueName() string {
	return d.queue.Name
}

func (d *DirectConsumer) GetKind() string {
	return d.kind
}

func (d *DirectConsumer) Consume(ctx context.Context) (string, uint64, error) {
	select {
	case <-d.hub.Done():
		log.Warn("hub done")
		return "", 0, ServerUnavailable
	case <-d.hub.AmqpConnDone():
		log.Warn("server amqp done")
		return "", 0, ServerUnavailable
	case <-ctx.Done():
		log.Warn("Consume client done")
		return "", 0, errors.New("client done")
	case data, ok := <-d.delivery:
		if ok {
			msg := &Message{}
			json.Unmarshal(data.Body, &msg)
			now := time.Now()
			if msg.RunAfter == nil {
				msg.RunAfter = &now
			}
			sra := msg.RunAfter.Add(time.Second * time.Duration(d.hub.Config().MaxJobRunningSeconds))
			queue := &models.Queue{
				Data:       msg.Data,
				QueueName:  d.GetQueueName(),
				RetryTimes: msg.RetryTimes,
				Ref:        msg.Ref,
				RunAfter:   &sra,
			}
			d.hub.GetDBConn().Create(queue)
			data.Ack(false)
			return msg.Data, uint64(queue.ID), nil
		}
		return "", 0, ServerUnavailable
	}
}

func (d *DirectConsumer) PrepareDelivery() error {
	var (
		delivery <-chan amqp.Delivery
		err      error
	)
	if delivery, err = d.channel.Consume(d.GetQueueName(), "", false, false, false, false, nil); err != nil {
		return err
	}
	d.delivery = delivery

	return nil
}

func (d *DirectConsumer) PrepareQos() error {
	if err := d.channel.Qos(
		d.hub.Config().PrefetchCount, // prefetch count
		0,                            // prefetch size
		false,                        // global
	); err != nil {
		return err
	}

	return nil
}

func (d *DirectConsumer) PrepareConn() error {
	defer func(t time.Time) { log.Warn("DirectConsumer PrepareConn", time.Since(t)) }(time.Now())

	var (
		conn *amqp.Connection
		err  error
	)

	if conn, err = d.hub.GetAmqpConn(); err != nil {
		return err
	}

	d.conn = conn

	return nil
}

func (d *DirectConsumer) PrepareChannel() error {
	defer func(t time.Time) { log.Warn("DirectConsumer prepareChannel", time.Since(t)) }(time.Now())

	var (
		err error
	)

	if d.channel, err = d.conn.Channel(); err != nil {
		return err
	}

	return nil
}

func (d *DirectConsumer) PrepareExchange() error {
	defer func(t time.Time) { log.Warn("DirectConsumer prepareExchange", time.Since(t)) }(time.Now())

	var (
		err error
	)
	if err = d.channel.ExchangeDeclare(
		d.exchange,
		amqp.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}
	return nil
}

func (d *DirectConsumer) PrepareQueueDeclare() error {
	defer func(t time.Time) { log.Warn("DirectConsumer prepareQueueDeclare", time.Since(t)) }(time.Now())

	var (
		err   error
		queue amqp.Queue
	)
	if queue, err = d.channel.QueueDeclare(
		d.queueName,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}
	d.queue = queue

	return nil
}

func (d *DirectConsumer) PrepareQueueBind() error {
	defer func(t time.Time) { log.Warn("DirectConsumer prepareQueueBind", time.Since(t)) }(time.Now())

	var err error
	if err = d.channel.QueueBind(d.GetQueueName(), d.GetQueueName(), DefaultExchange, false, nil); err != nil {
		return err
	}
	return nil
}

func (d *DirectConsumer) Done() chan *amqp.Error {
	return d.closeChan
}

func (d *DirectConsumer) Build() (ConsumerInterface, error) {
	var (
		err error
	)

	if err = d.PrepareConn(); err != nil {
		log.Error("NewConsumer PrepareConn", err)
		return nil, err
	}

	if err = d.PrepareChannel(); err != nil {
		log.Error("NewConsumer prepareChannel", err)
		return nil, err
	}

	if err = d.PrepareExchange(); err != nil {
		log.Error("NewConsumer prepareExchange", err)
		return nil, err
	}

	if err = d.PrepareQueueDeclare(); err != nil {
		log.Error("NewConsumer prepareQueueDeclare", err)

		return nil, err
	}

	if err = d.PrepareQueueBind(); err != nil {
		log.Error("NewConsumer prepareQueueBind", err)

		return nil, err
	}

	if err = d.PrepareQos(); err != nil {
		log.Error("NewConsumer PrepareQos", err)

		return nil, err
	}

	if err = d.PrepareDelivery(); err != nil {
		return nil, err
	}
	d.closeChan = d.channel.NotifyClose(make(chan *amqp.Error))
	go func() {
		defer func() {
			d.Close()
			log.Warnf("CONSUMER EXIT %s", d.GetQueueName())
		}()
		select {
		case <-d.hub.Done():
			log.Info("new consumer hub ctx done")
		case <-d.Done():
		case <-d.hub.AmqpConnDone():
			log.Info("hub.notifyConnClose consumer")
		}
	}()

	return d, nil
}

func (d *DirectConsumer) Close() {
	if d.closed.isSet() {
		log.Infof("consumer %s is already closed.", d.GetQueueName())
		return
	}
	d.closed.setTrue()
	select {
	case <-d.hub.AmqpConnDone():
		if err := d.channel.Close(); err != nil {
			log.Errorf("Close channel err %v %s", err, d.GetQueueName())
		}
		//log.Warn("consumer closing but amqp conn done.")
	case <-d.Done():
		//log.Info("consumer closeChan done.")
	default:
		if err := d.channel.Close(); err != nil {
			log.Errorf("Close channel err %v %s", err, d.GetQueueName())
		}
	}
	d.cm.RemoveConsumer(d)

	log.Info("after consumer close ", d.GetQueueName())
}
