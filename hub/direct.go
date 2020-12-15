package hub

import (
	"context"
	"errors"
	json "github.com/json-iterator/go"
	"mq/models"
	"time"

	"github.com/rs/xid"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var _ ProducerInterface = (*DirectProducer)(nil)
var _ ConsumerInterface = (*DirectConsumer)(nil)
var _ MqConfigInterface = (*DirectConsumer)(nil)
var _ MqConfigInterface = (*DirectProducer)(nil)

type DirectProducer struct {
	*ProducerBase
}

func newDirectProducer(queueName, exchange string, hub Interface, id int64, opts ...Option) ProducerBuilder {
	d := &DirectProducer{ProducerBase: &ProducerBase{
		id:        id,
		pm:        hub.ProducerManager(),
		queueName: queueName,
		kind:      amqp.ExchangeDirect,
		hub:       hub,
		exchange:  exchange,
	}}

	for _, opt := range opts {
		opt(d)
	}

	return d
}

func (d *DirectProducer) WithConsumerAck(needAck bool) {
	panic("implement me")
}

func (d *DirectProducer) WithConsumerReBalance(needReBalance bool) {
	panic("implement me")
}

func (d *DirectProducer) WithExchangeDurable(durable bool) {
	d.exchangeDurable = durable
}

func (d *DirectProducer) WithExchangeAutoDelete(autoDelete bool) {
	d.exchangeAutoDelete = autoDelete
}

func (d *DirectProducer) WithQueueAutoDelete(autoDelete bool) {
	d.queueAutoDelete = autoDelete
}

func (d *DirectProducer) WithQueueDurable(durable bool) {
	d.queueDurable = durable
}

func (d *DirectProducer) GetId() int64 {
	return d.id
}

func (d *DirectProducer) DelayPublish(message MessageInterface) error {
	runAfter := time.Now().Add(time.Duration(message.GetDelaySeconds()) * time.Second)

	return d.hub.DelayPublishQueue(models.DelayQueue{
		ExpirationSeconds: message.GetMessageExpiration(),
		UniqueId:          xid.New().String(),
		Data:              message.GetData(),
		QueueName:         d.queueName,
		RunAfter:          &runAfter,
		DelaySeconds:      uint(message.GetDelaySeconds()),
		Kind:              d.kind,
		Exchange:          d.exchange,
		RoutingKey:        d.routingKey,
	})
}

func (d *DirectProducer) Publish(msg MessageInterface) error {
	var (
		body []byte
		err  error
	)
	if msg.IsDelay() {
		return d.DelayPublish(msg)
	}

	msg.SetUniqueIdIfNotExist(xid.New().String())

	msg.SetExchange(d.exchange)
	msg.SetQueueName(d.queueName)
	msg.SetKind(d.kind)
	msg.SetRoutingKey(d.routingKey)

	if body, err = json.Marshal(&msg); err != nil {
		return err
	}

	select {
	case <-d.ChannelDone():
		return ErrorServerUnavailable
	case <-d.hub.Done():
		return ErrorServerUnavailable
	default:
		return d.channel.Publish(
			d.exchange,
			d.queueName,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
				Expiration:  msg.GetMessageExpirationString(),
			},
		)
	}
}

func (d *DirectProducer) ChannelDone() chan *amqp.Error {
	return d.closeChan
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

func (d *DirectProducer) GetExchange() string {
	return d.exchange
}

func (d *DirectProducer) GetRoutingKey() string {
	return d.routingKey
}

func (d *DirectProducer) PrepareConn() error {
	defer func(t time.Time) { log.Debugf("DirectProducer PrepareConn time: %v", time.Since(t)) }(time.Now())

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

func (d *DirectProducer) PrepareChannel() error {
	defer func(t time.Time) { log.Debugf("DirectProducer prepareChannel %v.", time.Since(t)) }(time.Now())

	var (
		err error
	)

	if d.channel, err = d.conn.Channel(); err != nil {
		return err
	}

	return nil
}

func (d *DirectProducer) PrepareExchange() error {
	defer func(t time.Time) { log.Debugf("DirectProducer prepareExchange %v.", time.Since(t)) }(time.Now())

	var (
		err error
	)
	if err = d.channel.ExchangeDeclare(
		d.exchange,
		d.kind,
		d.exchangeDurable,
		d.exchangeAutoDelete,
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
		log.Debugf("DirectProducer prepareQueueDeclare queueName %s %v", d.queueName, time.Since(t))
	}(time.Now())

	var (
		err   error
		queue amqp.Queue
	)

	if queue, err = d.channel.QueueDeclare(
		d.queueName,
		d.queueDurable,
		d.queueAutoDelete,
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
	defer func(t time.Time) { log.Debugf("DirectProducer prepareQueueBind %v.", time.Since(t)) }(time.Now())

	var err error
	if err = d.channel.QueueBind(d.queueName, d.queueName, d.exchange, false, nil); err != nil {
		return err
	}
	return nil
}

func (d *DirectProducer) Build() (ProducerInterface, error) {
	log.Infof("start build producer %s.", d.queueName)

	var (
		err error
	)

	if err = d.PrepareConn(); err != nil {
		log.Debug("newProducer PrepareConn", err)
		return nil, err
	}

	if err = d.PrepareChannel(); err != nil {
		log.Debug("newProducer prepareChannel", err)
		return nil, err
	}

	if err = d.PrepareExchange(); err != nil {
		log.Debug("newProducer prepareExchange", err)
		return nil, err
	}

	if err = d.PrepareQueueDeclare(); err != nil {
		log.Debug("newProducer prepareQueueDeclare", err)

		return nil, err
	}

	if err = d.PrepareQueueBind(); err != nil {
		log.Debug("newProducer prepareQueueBind", err)

		return nil, err
	}

	d.closeChan = d.channel.NotifyClose(make(chan *amqp.Error))

	go func() {
		defer func() {
			d.Close()
		}()

		select {
		case <-d.ChannelDone():
		case <-d.hub.Done():
		}
	}()

	return d, nil
}

func (d *DirectProducer) Close() {
	if d.closed.isSet() {
		log.Debugf("producer %s already closed.", d.GetQueueName())
		return
	}
	d.RemoveSelf()
	select {
	case <-d.ChannelDone():
	case <-d.hub.Done():
	default:
	}
	if err := d.channel.Close(); err != nil {
		log.Debugf("Close channel err %v %s", err, d.GetQueueName())
	}
	log.Infof("####### PRODUCER CLOSED queue: %s id: %d #######", d.GetQueueName(), d.GetId())
	d.closed.setTrue()
}

func (d *DirectProducer) RemoveSelf() {
	d.pm.RemoveProducer(d)
}

type DirectConsumer struct {
	*ConsumerBase
}

func newDirectConsumer(queueName, exchange string, hub Interface, id int64, opts ...Option) ConsumerBuilder {
	dc := &DirectConsumer{ConsumerBase: &ConsumerBase{
		id:        id,
		cm:        hub.ConsumerManager(),
		queueName: queueName,
		kind:      amqp.ExchangeDirect,
		hub:       hub,
		exchange:  exchange,
	}}

	for _, opt := range opts {
		opt(dc)
	}

	return dc
}

func (d *DirectConsumer) WithConsumerAck(needAck bool) {
	d.autoAck = !needAck
}

func (d *DirectConsumer) WithConsumerReBalance(needReBalance bool) {
	d.dontNeedReBalance = !needReBalance
}

func (d *DirectConsumer) WithExchangeDurable(durable bool) {
	d.exchangeDurable = durable
}

func (d *DirectConsumer) WithExchangeAutoDelete(autoDelete bool) {
	d.exchangeAutoDelete = autoDelete
}

func (d *DirectConsumer) WithQueueAutoDelete(autoDelete bool) {
	d.queueAutoDelete = autoDelete
}

func (d *DirectConsumer) WithQueueDurable(durable bool) {
	d.queueDurable = durable
}

func (d *DirectConsumer) Consume(ctx context.Context) (MessageInterface, error) {
	var (
		err error
		msg *Message
	)

	select {
	case <-d.hub.Done():
		log.Debug("hub done")
		return nil, ErrorServerUnavailable
	case <-ctx.Done():
		log.Debug("Consume client done")
		return nil, errors.New("client done")
	case data, ok := <-d.Delivery():
		if !ok {
			data.Nack(false, true)

			return nil, ErrorServerUnavailable
		}

		json.Unmarshal(data.Body, &msg)
		msg.SetRunAfter(nextRunTime(msg, d.hub.Config().MaxJobRunningSeconds))

		if !d.AutoAck() {
			if err = publishConfirmMsg(d.hub.(BackgroundJobWorker), msg); err != nil {
				data.Nack(false, true)
				return nil, err
			}
		}

		if err := data.Ack(false); err != nil {
			log.Debug(err)
			return nil, err
		}

		return msg, nil
	}
}

func publishConfirmMsg(w BackgroundJobWorker, msg *Message) error {
	var (
		ackProducer ProducerInterface
		err         error
	)

	if ackProducer, err = w.GetConfirmProducer(); err != nil {
		log.Debug(err)
		return err
	}

	marshal, err := json.Marshal(NewConfirmMessage(msg.GetUniqueId(), msg.GetData(), msg.GetQueueName(), msg.GetKind(), msg.GetExchange(), msg.GetRef(), msg.GetRunAfter(), msg.GetRetryTimes(), msg.GetRoutingKey()))

	if err := ackProducer.Publish(NewMessage(string(marshal))); err != nil {
		log.Debug(err)
		return err
	}

	return nil
}

func (d *DirectConsumer) Ack(uniqueId string) error {
	if d.AutoAck() {
		return d.hub.Ack(uniqueId)
	}

	return nil
}

func (d *DirectConsumer) Nack(uniqueId string) error {
	if !d.AutoAck() {
		return d.hub.Nack(uniqueId)
	}

	return nil
}

func (d *DirectConsumer) Delivery() chan amqp.Delivery {
	return d.cm.Delivery(getKey(d.queueName, d.kind, d.exchange, d.routingKey))
}

func (d *DirectConsumer) AutoAck() bool {
	return d.autoAck
}

func (d *DirectConsumer) GetId() int64 {
	return d.id
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

func (d *DirectConsumer) GetExchange() string {
	return d.exchange
}

func (d *DirectConsumer) GetRoutingKey() string {
	return d.routingKey
}

func (d *DirectConsumer) PrepareConn() error {
	defer func(t time.Time) { log.Debugf("DirectConsumer PrepareConn %v.", time.Since(t)) }(time.Now())

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

func (d *DirectConsumer) PrepareExchange() error {
	defer func(t time.Time) { log.Debugf("DirectConsumer prepareExchange %v.", time.Since(t)) }(time.Now())

	var (
		err error
	)
	if err = d.channel.ExchangeDeclare(
		d.exchange,
		d.kind,
		d.exchangeDurable,
		d.exchangeAutoDelete,
		false,
		false,
		nil,
	); err != nil {
		return err
	}
	return nil
}

func (d *DirectConsumer) PrepareQueueDeclare() error {
	defer func(t time.Time) { log.Debugf("DirectConsumer prepareQueueDeclare %v.", time.Since(t)) }(time.Now())

	var (
		err   error
		queue amqp.Queue
	)
	if queue, err = d.channel.QueueDeclare(
		d.queueName,
		d.queueDurable,
		d.queueAutoDelete,
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
	defer func(t time.Time) { log.Debugf("DirectConsumer prepareQueueBind %v.", time.Since(t)) }(time.Now())

	var err error
	if err = d.channel.QueueBind(d.GetQueueName(), d.GetQueueName(), d.exchange, false, nil); err != nil {
		return err
	}
	return nil
}

func (d *DirectConsumer) PrepareQos() error {
	if d.hub.Config().PrefetchCount == 0 {
		return nil
	}

	if err := d.channel.Qos(
		d.hub.Config().PrefetchCount, // prefetch count
		0,                            // prefetch size
		false,                        // global
	); err != nil {
		return err
	}

	return nil
}

func (d *DirectConsumer) PrepareDelivery() error {
	var (
		delivery <-chan amqp.Delivery
		err      error
	)
	if delivery, err = d.channel.Consume(d.queueName, "", d.autoAck, false, false, false, nil); err != nil {
		return err
	}
	d.delivery = delivery

	return nil
}

func (d *DirectConsumer) PrepareChannel() error {
	defer func(t time.Time) { log.Debugf("DirectConsumer prepareChannel %v.", time.Since(t)) }(time.Now())

	var (
		err error
	)

	if d.channel, err = d.conn.Channel(); err != nil {
		return err
	}

	return nil
}

func (d *DirectConsumer) ChannelDone() chan *amqp.Error {
	return d.closeChan
}

func (d *DirectConsumer) Build() (ConsumerInterface, error) {
	log.Infof("start build consumer %s.", d.queueName)
	var (
		err error
	)

	if err = d.PrepareConn(); err != nil {
		log.Debug("newConsumer PrepareConn", err)
		return nil, err
	}

	if err = d.PrepareChannel(); err != nil {
		log.Debug("newConsumer prepareChannel", err)
		return nil, err
	}

	if err = d.PrepareExchange(); err != nil {
		log.Debug("newConsumer prepareExchange", err)
		return nil, err
	}

	if err = d.PrepareQueueDeclare(); err != nil {
		log.Debug("newConsumer prepareQueueDeclare", err)

		return nil, err
	}

	if err = d.PrepareQueueBind(); err != nil {
		log.Debug("newConsumer prepareQueueBind", err)

		return nil, err
	}

	if err = d.PrepareQos(); err != nil {
		log.Debug("newConsumer PrepareQos", err)

		return nil, err
	}

	if err = d.PrepareDelivery(); err != nil {
		return nil, err
	}
	d.closeChan = d.channel.NotifyClose(make(chan *amqp.Error))
	go func() {
		defer func() {
			d.Close()
		}()
		select {
		case <-d.hub.Done():
			log.Debug("new consumer hub ctx done")
		case <-d.ChannelDone():
		}
	}()

	go func() {
		defer log.Warnf("exchange %s 队列 %s 的 consumer %d go Delivery EXIT", d.GetExchange(), d.GetQueueName(), d.GetId())
		log.Debugf("exchange %s 队列 %s 的 consumer %d 往公共 Delivery 推消息", d.GetExchange(), d.GetQueueName(), d.GetId())

		for {
			select {
			case data, ok := <-d.delivery:
				if !ok {
					return
				}

				d.Delivery() <- data
			case <-d.hub.Done():
				return
			case <-d.ChannelDone():
				return
			}
		}
	}()

	return d, nil
}

func (d *DirectConsumer) Close() {
	if d.closed.isSet() {
		log.Warnf("consumer %s is already closed.", d.GetQueueName())
		return
	}
	d.RemoveSelf()
	select {
	case <-d.hub.Done():
	case <-d.ChannelDone():
	default:
	}

	if err := d.channel.Close(); err != nil {
		log.Debugf("Close channel err %v %s", err, d.GetQueueName())
	}

	log.Debugf("############ CONSUMER CLOSED queue: %s id: %d ############", d.GetQueueName(), d.GetId())
	d.closed.setTrue()
}

func (d *DirectConsumer) RemoveSelf() {
	d.cm.RemoveConsumer(d)
}

func (d *DirectConsumer) DontNeedReBalance() bool {
	return d.dontNeedReBalance
}

func nextRunTime(msg *Message, maxJobRunSeconds uint) *time.Time {
	if msg.GetRunAfter() != nil {
		return msg.GetRunAfter()
	}

	next := time.Now().Add(time.Duration(maxJobRunSeconds) * time.Second)

	return &next
}
