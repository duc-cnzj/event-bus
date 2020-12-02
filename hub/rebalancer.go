package hub

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
)

var RecheckExchange = "recheck_exchange"

type RecheckMessage struct {
	QueueName string
	Exchange  string
	Kind      string
	Host      string
}

type Rebalancer struct {
	hub *Hub
}

func NewRebalancer(hub *Hub) *Rebalancer {
	rb := &Rebalancer{hub: hub}

	rb.ListenQueue()

	return rb
}

func (r *Rebalancer) CheckQueue(queueName, kind, exchange string) {
	var (
		producer ProducerInterface
		err      error
	)
	if producer, err = r.hub.pm.GetProducer("", amqp.ExchangeFanout, RecheckExchange); err != nil {
		log.Error(err)
		return
	}

	hostname, _ := os.Hostname()
	marshal, _ := json.Marshal(&RecheckMessage{QueueName: queueName, Host: hostname, Kind: kind, Exchange: exchange})
	producer.Publish(Message{Data: string(marshal)})
}

func (r *Rebalancer) ListenQueue() {
	log.Info("LISTEN Rebalance!")
	var (
		consumer ConsumerInterface
		err      error
	)
	hostname, _ := os.Hostname()
	if consumer, err = r.hub.cm.GetConsumer(hostname, amqp.ExchangeFanout, RecheckExchange); err != nil {
		log.Error(err)
		return
	}

	// todo 30秒内不再触发重平衡
	go func() {
		for {
			select {
			case <-r.hub.Done():
				log.Error("rb hub done。")
				return
			case delivery, ok := <-consumer.Delivery():
				var (
					msg        Message
					recheckMsg RecheckMessage
				)
				delivery.Ack(true)
				if !ok {
					log.Info("not okkk")
					break
				}
				if err = json.Unmarshal(delivery.Body, &msg); err != nil {
					log.Error(err)
					break
				}
				if err = json.Unmarshal([]byte(msg.GetData()), &recheckMsg); err != nil {
					log.Error(err)
					break
				}
				if recheckMsg.Host == hostname {
					log.Infof("hostname same ignore %s", recheckMsg.Host)
					break
				}
				log.Infof("收到重平衡消息 queueName: %s 队列长度：%d host: %s", recheckMsg.QueueName, len(r.hub.cm.Delivery(recheckMsg.QueueName, recheckMsg.Kind, recheckMsg.Exchange)), hostname)
				if len(r.hub.cm.Delivery(recheckMsg.QueueName, recheckMsg.Kind, recheckMsg.Exchange)) > 0 {
					log.Warnf("触发重平衡 %s host: %s", recheckMsg.QueueName, hostname)
					for {
						select {
						case d := <-r.hub.cm.Delivery(recheckMsg.QueueName, recheckMsg.Kind, recheckMsg.Exchange):
							d.Nack(false, true)
						default:
							break
						}
					}
				}
			}
		}
	}()
}
