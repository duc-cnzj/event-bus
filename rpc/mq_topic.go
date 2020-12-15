package rpc

import (
	"context"
	"errors"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"mq/hub"
	mq "mq/protos"
)

type MQTopic struct {
	mq.UnimplementedMqTopicServer
	Hub hub.Interface
}

// 推送消息
func (m *MQTopic) Publish(ctx context.Context, pub *mq.TopicPublishRequest) (*empty.Empty, error) {
	log.Debug("publish", pub.Data, pub.Topic)
	var (
		producer hub.ProducerInterface
		err      error
	)

	if pub.Topic == "" || pub.Data == "" {
		return nil, errors.New("queue and data can't empty")
	}

	if producer, err = m.newProducer(pub.Topic); err != nil {
		return nil, err
	}

	if err := producer.Publish(hub.NewMessage(pub.Data).SetMessageExpiration(uint(pub.Expiration))); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

// DelayPublish 延迟推送
func (m *MQTopic) DelayPublish(ctx context.Context, req *mq.DelayTopicPublishRequest) (*empty.Empty, error) {
	log.Debug("delay publish", req.Topic)
	var (
		err      error
		producer hub.ProducerInterface
	)

	if producer, err = m.newProducer(req.Topic); err != nil {
		return nil, err
	}

	if err = producer.DelayPublish(
		hub.NewMessage(req.Data).
			Delay(uint(req.DelaySeconds)).
			SetMessageExpiration(uint(req.Expiration)),
	); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

// Subscribe 订阅消息
func (m *MQTopic) Subscribe(ctx context.Context, sub *mq.TopicSubscribeRequest) (*mq.SubscribeResponse, error) {
	log.Debug("Subscribe", sub.Topic)
	var (
		consumer hub.ConsumerInterface
		err      error
		msg      hub.MessageInterface
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if consumer, err = m.newConsumer(sub.QueueName, sub.Topic); err != nil {
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
func (m *MQTopic) Ack(ctx context.Context, queueId *mq.QueueId) (*empty.Empty, error) {
	log.Debug("Ack", queueId.Id)

	if err := m.Hub.Ack(queueId.GetId()); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

// Nack 客户端拒绝消费
func (m *MQTopic) Nack(ctx context.Context, queueId *mq.QueueId) (*empty.Empty, error) {
	log.Debug("Nack", queueId.Id)

	if err := m.Hub.Nack(queueId.GetId()); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (m *MQTopic) mustEmbedUnimplementedMQServer() {
	panic("implement me")
}

func (m *MQTopic) newProducer(exchange string) (hub.ProducerInterface, error) {
	return m.Hub.NewDurableNotAutoDeleteTopicProducer(exchange, exchange)
}

func (m *MQTopic) newConsumer(queueName, exchange string) (hub.ConsumerInterface, error) {
	return m.Hub.NewDurableNotAutoDeleteTopicConsumer(queueName, exchange, exchange)
}
