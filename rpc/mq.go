package rpc

import (
	"context"
	"errors"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"mq/hub"
	mq "mq/protos"
)

type MQ struct {
	mq.UnimplementedMqServer
	Hub hub.Interface
}

// 推送消息
func (m *MQ) Publish(ctx context.Context, pub *mq.PublishRequest) (*empty.Empty, error) {
	log.Debug("publish", pub.Data, pub.Queue)
	var (
		producer hub.ProducerInterface
		err      error
	)

	if pub.Queue == "" || pub.Data == "" {
		return nil, errors.New("queue and data can't empty")
	}

	if producer, err = m.newProducer(pub.Queue); err != nil {
		return nil, err
	}

	if err := producer.Publish(hub.NewMessage(pub.Data)); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

// DelayPublish 延迟推送
func (m *MQ) DelayPublish(ctx context.Context, req *mq.DelayPublishRequest) (*empty.Empty, error) {
	log.Debug("delay publish", req.Queue)
	var (
		err      error
		producer hub.ProducerInterface
	)

	if producer, err = m.newProducer(req.Queue); err != nil {
		return nil, err
	}

	if err = producer.DelayPublish(hub.NewMessage(req.Data).Delay(uint(req.DelaySeconds))); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

// Subscribe 订阅消息
func (m *MQ) Subscribe(ctx context.Context, sub *mq.SubscribeRequest) (*mq.SubscribeResponse, error) {
	log.Debug("Subscribe", sub.Queue)
	var (
		consumer hub.ConsumerInterface
		err      error
		msg      hub.MessageInterface
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if consumer, err = m.newConsumer(sub.Queue); err != nil {
		return nil, err
	}

	if msg, err = consumer.Consume(ctx); err != nil {
		return nil, err
	}

	return &mq.SubscribeResponse{
		Id:    msg.GetUniqueId(),
		Data:  msg.GetData(),
		Queue: msg.GetQueueName(),
	}, nil
}

// Ack 客户端确认已消费成功
func (m *MQ) Ack(ctx context.Context, queueId *mq.QueueId) (*empty.Empty, error) {
	log.Debug("Ack", queueId.Id)

	if err := m.Hub.Ack(queueId.GetId()); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

// Nack 客户端拒绝消费
func (m *MQ) Nack(ctx context.Context, queueId *mq.QueueId) (*empty.Empty, error) {
	log.Debug("Nack", queueId.Id)

	if err := m.Hub.Nack(queueId.GetId()); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (m *MQ) mustEmbedUnimplementedMQServer() {
	panic("implement me")
}

func (m *MQ) newProducer(queueName string) (hub.ProducerInterface, error) {
	return m.Hub.NewDurableNotAutoDeleteDirectProducer(queueName)
}

func (m *MQ) newConsumer(queueName string) (hub.ConsumerInterface, error) {
	return m.Hub.NewDurableNotAutoDeleteDirectConsumer(queueName, true)
}
