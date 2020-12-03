package hub

import (
	"context"
	"errors"
	json "github.com/json-iterator/go"
	"github.com/rs/xid"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

var _ ProducerInterface = (*PubProducer)(nil)
var _ ConsumerInterface = (*SubConsumer)(nil)
var _ MqConfigInterface = (*SubConsumer)(nil)
var _ MqConfigInterface = (*PubProducer)(nil)

type PubProducer struct {
	*ProducerBase
}

func (p *PubProducer) WithConsumerAck(needAck bool) {
	panic("implement me")
}

func (p *PubProducer) WithExchangeDurable(durable bool) {
	p.exchangeDurable = durable
}

func (p *PubProducer) WithExchangeAutoDelete(autoDelete bool) {
	p.exchangeAutoDelete = autoDelete
}

func (p *PubProducer) WithQueueAutoDelete(autoDelete bool) {
	p.queueAutoDelete = autoDelete
}

func (p *PubProducer) WithQueueDurable(durable bool) {
	p.queueDurable = durable
}

func newPubProducer(exchange string, hub Interface, id int64, opts ...Option) ProducerBuilder {
	d := &PubProducer{ProducerBase: &ProducerBase{
		id:       id,
		pm:       hub.ProducerManager(),
		kind:     amqp.ExchangeFanout,
		hub:      hub,
		exchange: exchange,
	}}

	for _, opt := range opts {
		opt(d)
	}

	return d
}

func (p *PubProducer) PrepareConn() error {
	defer func(t time.Time) { log.Debugf("PubProducer PrepareConn time: %v", time.Since(t)) }(time.Now())

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

func (p *PubProducer) PrepareChannel() error {
	defer func(t time.Time) { log.Debugf("PubProducer prepareChannel %v.", time.Since(t)) }(time.Now())

	var (
		err error
	)

	if p.channel, err = p.conn.Channel(); err != nil {
		return err
	}

	return nil
}

func (p *PubProducer) PrepareExchange() error {
	defer func(t time.Time) { log.Debugf("PubProducer prepareExchange %v.", time.Since(t)) }(time.Now())

	var (
		err error
	)
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

func (p *PubProducer) PrepareQueueDeclare() error {
	return nil
}

func (p *PubProducer) PrepareQueueBind() error {
	return nil
}

func (p *PubProducer) Build() (ProducerInterface, error) {
	log.Infof("start build producer %s.", p.queueName)

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

func (p *PubProducer) GetId() int64 {
	return p.id
}

func (p *PubProducer) GetConn() *amqp.Connection {
	return p.conn
}

func (p *PubProducer) GetChannel() *amqp.Channel {
	return p.channel
}

func (p *PubProducer) GetQueueName() string {
	return p.queueName
}

func (p *PubProducer) GetKind() string {
	return p.kind
}
func (p *PubProducer) GetExchange() string {
	return p.exchange
}

func (p *PubProducer) DelayPublish(s string, message Message, u uint) error {
	panic("implement me")
}

func (p *PubProducer) Publish(message Message) error {
	var (
		body []byte
		err  error
	)
	if message.QueueName == "" {
		message.QueueName = p.GetQueueName()
	}
	if message.UniqueId == "" {
		message.UniqueId = xid.New().String()
	}

	if body, err = json.Marshal(&message); err != nil {
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
			"",
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			},
		)
	}
}

func (p *PubProducer) RemoveSelf() {
	p.pm.RemoveProducer(p)
}

func (p *PubProducer) Close() {
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

func (p *PubProducer) ChannelDone() chan *amqp.Error {
	return p.closeChan
}

type SubConsumer struct {
	*ConsumerBase
}

func newSubConsumer(queueName, exchange string, hub Interface, id int64, opts ...Option) ConsumerBuilder {
	sc := &SubConsumer{ConsumerBase: &ConsumerBase{
		id:        id,
		cm:        hub.ConsumerManager(),
		queueName: queueName,
		kind:      amqp.ExchangeFanout,
		hub:       hub,
		exchange:  exchange,
	}}

	for _, opt := range opts {
		opt(sc)
	}

	return sc
}

func (d *SubConsumer) WithConsumerAck(needAck bool) {
	d.autoAck = !needAck
}

func (d *SubConsumer) WithExchangeDurable(durable bool) {
	d.exchangeDurable = durable
}

func (d *SubConsumer) WithExchangeAutoDelete(autoDelete bool) {
	d.exchangeAutoDelete = autoDelete
}

func (d *SubConsumer) WithQueueAutoDelete(autoDelete bool) {
	d.queueAutoDelete = autoDelete
}

func (d *SubConsumer) WithQueueDurable(durable bool) {
	d.queueDurable = durable
}

func (d *SubConsumer) Ack(uniqueId string) error {
	if d.AutoAck() {
		return d.hub.Ack(uniqueId)
	}

	return nil
}

func (d *SubConsumer) Nack(uniqueId string) error {
	if d.AutoAck() {
		return d.hub.Nack(uniqueId)
	}

	return nil
}

func (d *SubConsumer) Delivery() chan amqp.Delivery {
	return d.cm.Delivery(getKey(d.queueName, d.kind, d.exchange))
}

func (d *SubConsumer) AutoAck() bool {
	return d.autoAck
}

func (d *SubConsumer) GetId() int64 {
	return d.id
}

func (d *SubConsumer) GetConn() *amqp.Connection {
	return d.conn
}

func (d *SubConsumer) GetChannel() *amqp.Channel {
	return d.channel
}

func (d *SubConsumer) GetQueueName() string {
	return d.queue.Name
}

func (d *SubConsumer) GetKind() string {
	return d.kind
}

func (d *SubConsumer) GetExchange() string {
	return d.exchange
}

func (d *SubConsumer) Consume(ctx context.Context) (*Message, error) {
	var (
		ackProducer ProducerInterface
		err         error
		msg         = &Message{}
	)

	recheckCtx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	go func() {
		select {
		case <-time.After(5 * time.Second):
			d.hub.CheckQueue(d.queueName, d.kind, d.exchange)
			log.Warnf("队列 %s 触发重平衡", d.queueName)
		case <-recheckCtx.Done():
			log.Debug("CheckQueue 未触发 exit")
		}
	}()

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
		msg.RunAfter = nextRunTime(msg, d.hub.Config())

		if d.AutoAck() {
			if ackProducer, err = d.hub.GetConfirmProducer(); err != nil {
				log.Debug(err)
				data.Nack(false, true)
				return nil, err
			}

			if err := ackProducer.Publish(*msg); err != nil {
				log.Debug(err)
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

func (d *SubConsumer) PrepareConn() error {
	defer func(t time.Time) { log.Debugf("SubConsumer PrepareConn %v.", time.Since(t)) }(time.Now())

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

func (d *SubConsumer) PrepareExchange() error {
	defer func(t time.Time) { log.Debugf("SubConsumer prepareExchange %v.", time.Since(t)) }(time.Now())

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

func (d *SubConsumer) PrepareChannel() error {
	defer func(t time.Time) { log.Debugf("SubConsumer prepareChannel %v.", time.Since(t)) }(time.Now())

	var (
		err error
	)

	if d.channel, err = d.conn.Channel(); err != nil {
		return err
	}

	return nil
}

func (d *SubConsumer) PrepareQos() error {
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

func (d *SubConsumer) PrepareQueueDeclare() error {
	defer func(t time.Time) { log.Debugf("SubConsumer prepareQueueDeclare %v.", time.Since(t)) }(time.Now())

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

func (d *SubConsumer) PrepareQueueBind() error {
	defer func(t time.Time) { log.Debugf("SubConsumer prepareQueueBind %v.", time.Since(t)) }(time.Now())

	var err error
	if err = d.channel.QueueBind(d.GetQueueName(), d.GetQueueName(), d.exchange, false, nil); err != nil {
		return err
	}
	return nil
}

func (d *SubConsumer) PrepareDelivery() error {
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

func (d *SubConsumer) ChannelDone() chan *amqp.Error {
	return d.closeChan
}

func (d *SubConsumer) Build() (ConsumerInterface, error) {
	log.Infof("start build consumer %s.", d.queueName)
	var (
		err error
	)

	if err = d.PrepareConn(); err != nil {
		log.Error("SubConsumer PrepareConn", err)
		return nil, err
	}

	if err = d.PrepareChannel(); err != nil {
		log.Error("SubConsumer prepareChannel", err)
		return nil, err
	}

	if err = d.PrepareExchange(); err != nil {
		log.Error("SubConsumer prepareExchange", err)
		return nil, err
	}

	if err = d.PrepareQueueDeclare(); err != nil {
		log.Error("SubConsumer prepareQueueDeclare", err)

		return nil, err
	}

	if err = d.PrepareQueueBind(); err != nil {
		log.Error("SubConsumer prepareQueueBind", err)

		return nil, err
	}

	if err = d.PrepareQos(); err != nil {
		log.Error("SubConsumer PrepareQos", err)

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

func (d *SubConsumer) Close() {
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

func (d *SubConsumer) RemoveSelf() {
	d.cm.RemoveConsumer(d)
}
