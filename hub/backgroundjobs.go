package hub

import (
	"time"

	"github.com/DuC-cnZj/event-bus/models"
	json "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

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

func (h *Hub) Run() {
	go h.ConsumeConfirmQueue()
	go h.ConsumeAckQueue()
	go h.ConsumeDelayPublishQueue()
}

// 处理延迟推送队列
func (h *Hub) GetDelayPublishProducer() (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if producer, err = h.NewDurableNotAutoDeleteDirectProducer(DelayQueueName); err != nil {
		return nil, err
	}

	return producer, nil
}
func (h *Hub) GetDelayPublishConsumer() (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	if consumer, err = h.NewDurableNotAutoDeleteDirectConsumer(DelayQueueName, false); err != nil {
		return nil, err
	}

	return consumer, nil
}
func (h *Hub) ConsumeDelayPublishQueue() {
	for i := 0; i < h.Config().BackConsumerGoroutineNum; i++ {
		go h.consumeDelayPublishQueue()
	}
	log.Debug("back consume confirm queue started.")
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
				dp  models.DelayQueue
			)
			if err = json.Unmarshal(delivery.Body, &msg); err != nil {
				log.Error(err)
				delivery.Nack(false, true)
				return
			}

			if err = json.Unmarshal([]byte(msg.GetData()), &dp); err != nil {
				log.Error(err)
				delivery.Nack(false, true)
				return
			}

			if err = h.GetDBConn().Create(&dp).Error; err != nil {
				delivery.Nack(false, true)
				return
			}
			delivery.Ack(false)
		}
	}
}

//处理 confirm 队列
func (h *Hub) GetConfirmProducer() (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if producer, err = h.NewDurableNotAutoDeleteDirectProducer(ConfirmQueueName); err != nil {
		return nil, err
	}

	return producer, nil
}
func (h *Hub) GetConfirmConsumer() (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	if consumer, err = h.NewDurableNotAutoDeleteDirectConsumer(ConfirmQueueName, false); err != nil {
		return nil, err
	}

	return consumer, nil
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

			handleConfirm(h.GetDBConn(), delivery)
		}
	}
}
func handleConfirm(db *gorm.DB, delivery amqp.Delivery) {
	var (
		msg         = &Message{}
		err         error
		now         = time.Now()
		confirmData ConfirmMessage
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

	if err = json.Unmarshal([]byte(msg.GetData()), &confirmData); err != nil {
		log.Error(err)
		return
	}

	var queue models.Queue
	json.Unmarshal([]byte(msg.GetData()), &msg)

	if err = db.Unscoped().Model(&models.Queue{}).Where("unique_id", confirmData.UniqueId).First(&queue).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			if err = db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "unique_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"routing_key", "exchange", "kind", "run_after", "retry_times", "confirmed_at", "Data", "queue_name", "Ref", "is_confirmed"}),
			}).Create(&models.Queue{
				Exchange:    confirmData.Exchange,
				Kind:        confirmData.Kind,
				UniqueId:    confirmData.UniqueId,
				RetryTimes:  confirmData.RetryTimes,
				ConfirmedAt: &now,
				Data:        confirmData.Data,
				QueueName:   confirmData.QueueName,
				RoutingKey:  confirmData.RoutingKey,
				RunAfter:    confirmData.RunAfter,
				Ref:         confirmData.Ref,
				IsConfirmed: true,
			}).Error; err != nil {
				log.Error(err)
			}
		} else {
			log.Error(err)
		}
		return
	}

	if queue.Deleted() {
		log.Warnf("queue %s already deleted queueName %s kind %s", queue.UniqueId, queue.QueueName, queue.Kind)
		return
	}

	if queue.Nackd() {
		if !queue.Confirmed() {
			if err = db.Model(&models.Queue{ID: queue.ID}).Updates(&models.Queue{
				Kind:        confirmData.Kind,
				Exchange:    confirmData.Exchange,
				RetryTimes:  confirmData.RetryTimes,
				ConfirmedAt: &now,
				Data:        confirmData.Data,
				QueueName:   confirmData.QueueName,
				RoutingKey:  confirmData.RoutingKey,
				RunAfter:    confirmData.RunAfter,
				Ref:         confirmData.Ref,
				IsConfirmed: true,
			}).Error; err != nil {
				log.Error(err)
			}
		}
		log.Debug("queue status ", queue.NackedAt)
		return
	}

	if !queue.Confirmed() {
		if err = db.Model(&models.Queue{ID: queue.ID}).Updates(&models.Queue{
			Exchange:    confirmData.Exchange,
			Kind:        confirmData.Kind,
			RetryTimes:  confirmData.RetryTimes,
			RoutingKey:  confirmData.RoutingKey,
			ConfirmedAt: &now,
			Data:        confirmData.Data,
			QueueName:   confirmData.QueueName,
			RunAfter:    confirmData.RunAfter,
			Ref:         confirmData.Ref,
			IsConfirmed: true,
		}).Error; err != nil {
			log.Error(err)
		}
	}
}

//处理 ack 队列
func (h *Hub) GetAckQueueConsumer() (ConsumerInterface, error) {
	var (
		consumer ConsumerInterface
		err      error
	)

	if consumer, err = h.NewDurableNotAutoDeleteDirectConsumer(AckQueueName, false); err != nil {
		return nil, err
	}

	return consumer, nil
}
func (h *Hub) GetAckQueueProducer() (ProducerInterface, error) {
	var (
		producer ProducerInterface
		err      error
	)

	if producer, err = h.NewDurableNotAutoDeleteDirectProducer(AckQueueName); err != nil {
		return nil, err
	}

	return producer, nil
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

			handleAck(h.GetDBConn(), delivery)
		}
	}
}
func handleAck(db *gorm.DB, delivery amqp.Delivery) {
	var (
		msg   = &Message{}
		err   error
		queue models.Queue
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

	var ackData AckMessage
	err = json.Unmarshal([]byte(msg.GetData()), &ackData)
	if err != nil {
		log.Error(err)
		return
	}

	if err = db.Unscoped().Model(&models.Queue{}).Where("unique_id", ackData.UniqueId).First(&queue).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			if err = db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "unique_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"acked_at", "status"}),
			}).Create(&models.Queue{
				UniqueId: ackData.UniqueId,
				AckedAt:  &ackData.AckedAt,
				Status:   models.StatusAcked,
			}).Error; err != nil {
				log.Error(err)
			}
		} else {
			log.Error(err)
		}
		return
	}

	if queue.Deleted() {
		log.Warnf("queue %s already deleted queueName %s kind %s", queue.UniqueId, queue.QueueName, queue.Kind)
		return
	}

	if queue.Nackd() {
		log.Debug("queue status ", queue.NackedAt)
		return
	}

	if err = db.Model(&models.Queue{ID: queue.ID}).Updates(&models.Queue{AckedAt: &ackData.AckedAt, Status: models.StatusAcked}).Error; err != nil {
		log.Error(err)
	}
}
