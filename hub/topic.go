package hub

import (
	"context"
	"errors"
	json "github.com/json-iterator/go"
	"github.com/rs/xid"
	"mq/models"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

// todo topic consumer 需要监听 topic|topic.queueName 两个队列  topic.queueName 用于重发
var _ ProducerInterface = (*TopicProducer)(nil)
var _ ConsumerInterface = (*TopicConsumer)(nil)
var _ MqConfigInterface = (*TopicConsumer)(nil)
var _ MqConfigInterface = (*TopicProducer)(nil)

type TopicProducer struct {
	*ProducerBase
}

func (p *TopicProducer) DelayPublish(message MessageInterface) error {
	runAfter := time.Now().Add(time.Duration(message.GetDelaySeconds()) * time.Second)

	return p.hub.DelayPublishQueue(models.DelayQueue{
		UniqueId:     xid.New().String(),
		Data:         message.GetData(),
		QueueName:    p.queueName,
		RoutingKey:   p.routingKey,
		RunAfter:     &runAfter,
		DelaySeconds: uint(message.GetDelaySeconds()),
		Kind:         p.kind,
		Exchange:     p.exchange,
	})
}

func (p *TopicProducer) WithConsumerAck(needAck bool) {
	panic("implement me")
}

func (p *TopicProducer) WithConsumerReBalance(needReBalance bool) {
	panic("implement me")
}

func (p *TopicProducer) WithExchangeDurable(durable bool) {
	p.exchangeDurable = durable
}

func (p *TopicProducer) WithExchangeAutoDelete(autoDelete bool) {
	p.exchangeAutoDelete = autoDelete
}

func (p *TopicProducer) WithQueueAutoDelete(autoDelete bool) {
	p.queueAutoDelete = autoDelete
}

func (p *TopicProducer) WithQueueDurable(durable bool) {
	p.queueDurable = durable
}

func newTopicProducer(exchange, routingKey string, hub Interface, id int64, opts ...Option) ProducerBuilder {
	d := &TopicProducer{ProducerBase: &ProducerBase{
		id:         id,
		pm:         hub.ProducerManager(),
		kind:       amqp.ExchangeTopic,
		hub:        hub,
		exchange:   exchange,
		routingKey: routingKey,
	}}

	log.Warn(routingKey)

	for _, opt := range opts {
		opt(d)
	}

	return d
}

func (p *TopicProducer) PrepareConn() error {
	defer func(t time.Time) { log.Debugf("TopicProducer PrepareConn time: %v", time.Since(t)) }(time.Now())

	var (
		conn *amqp.Connection
		err  error
	)

	if conn, err = p.hub.GetAmqpConn(); err != nil {
		return err
	}

	p.conn = conn

	return nil
}

func (p *TopicProducer) PrepareChannel() error {
	defer func(t time.Time) { log.Debugf("TopicProducer prepareChannel %v.", time.Since(t)) }(time.Now())

	var err error

	if p.channel, err = p.conn.Channel(); err != nil {
		return err
	}

	return nil
}

func (p *TopicProducer) PrepareExchange() error {
	defer func(t time.Time) { log.Debugf("TopicProducer prepareExchange %v.", time.Since(t)) }(time.Now())

	var err error

	if err = p.channel.ExchangeDeclare(
		p.exchange,
		p.kind,
		p.exchangeDurable,
		p.exchangeAutoDelete,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	return nil
}

func (p *TopicProducer) PrepareQueueDeclare() error {
	return nil
}

func (p *TopicProducer) PrepareQueueBind() error {
	return nil
}

func (p *TopicProducer) Build() (ProducerInterface, error) {
	log.Infof("start build producer queueName %s kind %s exchange %s.", p.queueName, p.kind, p.exchange)

	var (
		err error
	)

	if err = p.PrepareConn(); err != nil {
		log.Info("newProducer PrepareConn", err)
		return nil, err
	}

	if err = p.PrepareChannel(); err != nil {
		log.Info("newProducer prepareChannel", err)
		return nil, err
	}

	if err = p.PrepareExchange(); err != nil {
		log.Info("newProducer prepareExchange", err)
		return nil, err
	}

	p.closeChan = p.channel.NotifyClose(make(chan *amqp.Error))

	go func() {
		defer func() {
			p.Close()
		}()

		select {
		case <-p.ChannelDone():
		case <-p.hub.Done():
		}
	}()

	return p, nil
}

func (p *TopicProducer) GetId() int64 {
	return p.id
}

func (p *TopicProducer) GetConn() *amqp.Connection {
	return p.conn
}

func (p *TopicProducer) GetChannel() *amqp.Channel {
	return p.channel
}

func (p *TopicProducer) GetQueueName() string {
	return p.queueName
}

func (p *TopicProducer) GetKind() string {
	return p.kind
}

func (p *TopicProducer) GetRoutingKey() string {
	return p.routingKey
}

func (p *TopicProducer) GetExchange() string {
	return p.exchange
}

func (p *TopicProducer) Publish(message MessageInterface) error {
	var (
		body []byte
		err  error
	)

	if message.IsDelay() {
		return p.DelayPublish(message)
	}

	message.SetUniqueIdIfNotExist(xid.New().String())
	message.SetExchange(p.exchange)
	message.SetQueueName(p.queueName)
	message.SetKind(p.kind)
	message.SetRoutingKey(p.routingKey)

	if body, err = json.Marshal(&message); err != nil {
		log.Error(err)
		return err
	}
	select {
	case <-p.ChannelDone():
		return ErrorServerUnavailable
	case <-p.hub.Done():
		return ErrorServerUnavailable
	default:
		return p.channel.Publish(
			p.exchange,
			p.routingKey,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
				Expiration:  message.GetMessageExpiration(),
			},
		)
	}
}

func (p *TopicProducer) RemoveSelf() {
	p.pm.RemoveProducer(p)
}

func (p *TopicProducer) Close() {
	if p.closed.isSet() {
		log.Warnf("producer %s already closed.", p.GetQueueName())
		return
	}
	p.RemoveSelf()
	select {
	case <-p.ChannelDone():
	case <-p.hub.Done():
	default:
	}
	if err := p.channel.Close(); err != nil {
		log.Debugf("Close channel err %v %s", err, p.GetQueueName())
	}
	log.Infof("####### PRODUCER CLOSED queue: %s id: %d #######", p.GetQueueName(), p.GetId())
	p.closed.setTrue()
}

func (p *TopicProducer) ChannelDone() chan *amqp.Error {
	return p.closeChan
}

type TopicConsumer struct {
	*ConsumerBase
}

func newTopicConsumer(queueName, exchange, routingKey string, hub Interface, id int64, opts ...Option) ConsumerBuilder {
	sc := &TopicConsumer{ConsumerBase: &ConsumerBase{
		id:         id,
		cm:         hub.ConsumerManager(),
		queueName:  queueName,
		kind:       amqp.ExchangeTopic,
		hub:        hub,
		exchange:   exchange,
		routingKey: routingKey,
	}}

	for _, opt := range opts {
		opt(sc)
	}

	return sc
}

func (d *TopicConsumer) WithConsumerAck(needAck bool) {
	d.autoAck = !needAck
}

func (d *TopicConsumer) WithConsumerReBalance(needReBalance bool) {
	d.dontNeedReBalance = !needReBalance
}

func (d *TopicConsumer) WithExchangeDurable(durable bool) {
	d.exchangeDurable = durable
}

func (d *TopicConsumer) WithExchangeAutoDelete(autoDelete bool) {
	d.exchangeAutoDelete = autoDelete
}

func (d *TopicConsumer) WithQueueAutoDelete(autoDelete bool) {
	d.queueAutoDelete = autoDelete
}

func (d *TopicConsumer) WithQueueDurable(durable bool) {
	d.queueDurable = durable
}

func (d *TopicConsumer) Ack(uniqueId string) error {
	if d.AutoAck() {
		return d.hub.Ack(uniqueId)
	}

	return nil
}

func (d *TopicConsumer) Nack(uniqueId string) error {
	if !d.AutoAck() {
		return d.hub.Nack(uniqueId)
	}

	return nil
}

func (d *TopicConsumer) Delivery() chan amqp.Delivery {
	return d.cm.Delivery(getKey(d.queueName, d.kind, d.exchange, d.routingKey))
}

func (d *TopicConsumer) AutoAck() bool {
	return d.autoAck
}

func (d *TopicConsumer) GetId() int64 {
	return d.id
}

func (d *TopicConsumer) GetConn() *amqp.Connection {
	return d.conn
}

func (d *TopicConsumer) GetChannel() *amqp.Channel {
	return d.channel
}

func (d *TopicConsumer) GetQueueName() string {
	return d.queue.Name
}

func (d *TopicConsumer) GetKind() string {
	return d.kind
}

func (d *TopicConsumer) GetExchange() string {
	return d.exchange
}

func (d *TopicConsumer) GetRoutingKey() string {
	return d.routingKey
}

func (d *TopicConsumer) Consume(ctx context.Context) (MessageInterface, error) {
	var (
		err error
		msg = &Message{}
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
		msg.SetRef(msg.GetRef())
		msg.SetUniqueId(xid.New().String())
		msg.SetQueueName(d.queueName)
		msg.SetRunAfter(nextRunTime(msg, d.hub.Config().NackdJobNextRunDelaySeconds))

		if !d.AutoAck() {
			if err = publishConfirmMsg(d.hub.(BackgroundJobWorker), msg); err != nil {
				log.Error(err)
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

func (d *TopicConsumer) PrepareConn() error {
	defer func(t time.Time) { log.Debugf("TopicConsumer PrepareConn %v.", time.Since(t)) }(time.Now())

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

func (d *TopicConsumer) PrepareExchange() error {
	defer func(t time.Time) { log.Debugf("TopicConsumer prepareExchange %v.", time.Since(t)) }(time.Now())

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

func (d *TopicConsumer) PrepareChannel() error {
	defer func(t time.Time) { log.Debugf("TopicConsumer prepareChannel %v.", time.Since(t)) }(time.Now())

	var (
		err error
	)

	if d.channel, err = d.conn.Channel(); err != nil {
		return err
	}

	return nil
}

func (d *TopicConsumer) PrepareQos() error {
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

func (d *TopicConsumer) PrepareQueueDeclare() error {
	defer func(t time.Time) { log.Debugf("TopicConsumer prepareQueueDeclare %v.", time.Since(t)) }(time.Now())

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

func (d *TopicConsumer) PrepareQueueBind() error {
	defer func(t time.Time) { log.Debugf("TopicConsumer prepareQueueBind %v.", time.Since(t)) }(time.Now())

	var err error
	// 监听两个key 一个是 topic 本身，另一个是 自身的队列
	if err = d.channel.QueueBind(d.GetQueueName(), d.routingKey, d.exchange, false, nil); err != nil {
		return err
	}

	if err = d.channel.QueueBind(d.GetQueueName(), GetSelfQueueRoutingKey(d.routingKey, d.queueName), d.exchange, false, nil); err != nil {
		return err
	}

	return nil
}

func (d *TopicConsumer) PrepareDelivery() error {
	var (
		delivery <-chan amqp.Delivery
		err      error
	)
	if delivery, err = d.channel.Consume(d.GetQueueName(), "", d.autoAck, false, false, false, nil); err != nil {
		return err
	}
	d.delivery = delivery

	return nil
}

func (d *TopicConsumer) ChannelDone() chan *amqp.Error {
	return d.closeChan
}

func (d *TopicConsumer) Build() (ConsumerInterface, error) {
	log.Infof("start build consumer %s.", d.queueName)
	var (
		err error
	)

	if err = d.PrepareConn(); err != nil {
		log.Error("TopicConsumer PrepareConn", err)
		return nil, err
	}

	if err = d.PrepareChannel(); err != nil {
		log.Error("TopicConsumer prepareChannel", err)
		return nil, err
	}

	if err = d.PrepareExchange(); err != nil {
		log.Error("TopicConsumer prepareExchange", err)
		return nil, err
	}

	if err = d.PrepareQueueDeclare(); err != nil {
		log.Error("TopicConsumer prepareQueueDeclare", err)

		return nil, err
	}

	if err = d.PrepareQueueBind(); err != nil {
		log.Error("TopicConsumer prepareQueueBind", err)

		return nil, err
	}

	if err = d.PrepareQos(); err != nil {
		log.Error("TopicConsumer PrepareQos", err)

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
			log.Info("new consumer hub ctx done")
		case <-d.ChannelDone():
		}
	}()

	go func() {
		defer log.Warnf("exchange %s 队列 %s 的 consumer %d go Delivery EXIT", d.GetExchange(), d.GetQueueName(), d.GetId())
		log.Infof("exchange %s 队列 %s 的 consumer %d 往公共 Delivery 推消息", d.GetExchange(), d.GetQueueName(), d.GetId())

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

func (d *TopicConsumer) Close() {
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

	log.Infof("############ CONSUMER CLOSED queue: %s id: %d ############", d.GetQueueName(), d.GetId())
	d.closed.setTrue()
}

func (d *TopicConsumer) RemoveSelf() {
	d.cm.RemoveConsumer(d)
}

func (d *TopicConsumer) DontNeedReBalance() bool {
	return d.dontNeedReBalance
}

func GetSelfQueueRoutingKey(routingKey string, queueName string) string {
	return routingKey + "@@@" + queueName
}
