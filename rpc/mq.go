package rpc

import (
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"mq/hub"
	"mq/models"
	mq "mq/protos"
)

type MQ struct {
	mq.UnimplementedMqServer
	Hub hub.Interface
}

func (m *MQ) NewProducer(queueName string) (hub.ProducerInterface, error) {
	return m.Hub.NewProducer(queueName, amqp.ExchangeDirect)
}

func (m *MQ) NewConsumer(queueName string) (hub.ConsumerInterface, error) {
	return m.Hub.NewConsumer(queueName, amqp.ExchangeDirect)
}

// DelayPublish 延迟推送
func (m *MQ) DelayPublish(ctx context.Context, req *mq.DelayPublishRequest) (*mq.Response, error) {
	log.Debug("delay publish", req.Queue)
	var (
		delayQueue *models.DelayQueue
		err        error
	)

	if delayQueue, err = m.Hub.DelayPublish(
		req.Queue,
		hub.Message{
			Data:         req.Data,
			DelaySeconds: uint(req.Seconds),
		},
	); err != nil {
		return nil, err
	}

	return &mq.Response{
		Success:      true,
		Data:         delayQueue.Data,
		Queue:        delayQueue.QueueName,
		Id:           delayQueue.UniqueId,
		RunAfter:     delayQueue.RunAfter.String(),
		DelaySeconds: uint64(delayQueue.DelaySeconds),
	}, nil
}

// Ack 客户端确认已消费成功
func (m *MQ) Ack(ctx context.Context, queueId *mq.QueueId) (*mq.Response, error) {
	log.Debug("Ack", queueId.Id)

	if err := m.Hub.Ack(queueId.GetId()); err != nil {
		return nil, err
	}

	return &mq.Response{
		Success: true,
	}, nil
}

// Nack 客户端拒绝消费
func (m *MQ) Nack(ctx context.Context, queueId *mq.QueueId) (*mq.Response, error) {
	log.Debug("Nack", queueId.Id)

	if err := m.Hub.Nack(queueId.GetId()); err != nil {
		return nil, err
	}

	return &mq.Response{
		Success: true,
	}, nil
}

func (m *MQ) Publish(ctx context.Context, pub *mq.PublishRequest) (*mq.Response, error) {
	log.Debug("publish", pub.Data, pub.Queue)
	if pub.Queue == "" || pub.Data == "" {
		return nil, errors.New("queue name and data can not be null")
	}
	var (
		producer hub.ProducerInterface
		err      error
	)
	if producer, err = m.NewProducer(pub.Queue); err != nil {
		return nil, errors.New("server unavailable")
	}
	if err := producer.Publish(hub.Message{Data: pub.Data}); err != nil {
		return nil, err
	}

	return &mq.Response{
		Success: true,
		Data:    pub.Data,
	}, nil
}

func (m *MQ) Subscribe(ctx context.Context, sub *mq.SubscribeRequest) (*mq.Response, error) {
	log.Debug("Subscribe", sub.Queue)
	var (
		consumer hub.ConsumerInterface
		err      error
		msg      *hub.Message
	)
	if consumer, err = m.NewConsumer(sub.Queue); err != nil {
		return nil, status.Errorf(codes.Unavailable, "server unavailable")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	msg, err = consumer.Consume(ctx)
	if err != nil {
		return nil, err
	}

	return &mq.Response{
		Success: true,
		Data:    msg.Data,
		Queue:   sub.Queue,
		Id:      msg.UniqueId,
	}, nil
}

func (m *MQ) mustEmbedUnimplementedMQServer() {
	panic("implement me")
}
