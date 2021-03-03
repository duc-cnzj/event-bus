package schedule

import (
	"sync"
	"time"

	dlm "github.com/DuC-cnZj/dlm"
	"github.com/DuC-cnZj/event-bus/hub"
	"github.com/DuC-cnZj/event-bus/models"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"gorm.io/gorm"
)

func Republish(h hub.Interface, lockList *sync.Map) func() {
	log.Info("Republish job running.")

	return func() {
		lock := dlm.NewLock(app.Redis(), "republish", dlm.WithEX(app.Config().DLMExpiration))
		if lock.Acquire() {
			var (
				queues        []*models.Queue
				producer      hub.ProducerInterface
				err           error
				lastAckdQueue models.Queue
				runtimeDelay  time.Duration
			)

			lockList.Store(lock.GetCurrentOwner(), lock)
			defer func(t time.Time) {
				lockList.Delete(lock.GetCurrentOwner())
				lock.Release()
				if len(queues) > 0 {
					log.Infof("SUCCESS consume republish len: %d , time is %s", len(queues), time.Since(t))
				}
			}(time.Now())

			log.Debug("[SUCCESS]: cron republish")

			// 获取最近队列延迟的时差
			if err = app.DB().
				Where("status = ?", models.StatusAcked).
				Where("is_confirmed = true").
				Where("updated_at < ?", time.Now().Add(-time.Minute*5)).
				Order("id DESC").
				First(&lastAckdQueue).Error; err != nil {
				if err != gorm.ErrRecordNotFound {
					log.Error(err)
				}
			} else {
				runtimeDelay = time.Second * time.Duration(lastAckdQueue.UpdatedAt.Sub(*lastAckdQueue.AckedAt).Seconds())
			}

			if runtimeDelay > time.Minute*30 {
				log.Warnf("消费队列延迟超过 %s > 30m 请注意!", runtimeDelay.String())
				runtimeDelay = time.Minute * 30
			}

			log.Debug("runtimeDelay", runtimeDelay, time.Now().Add(-runtimeDelay))

			if err = app.DB().
				Where("is_confirmed = true").
				Where("status in (?, ?)", models.StatusNacked, models.StatusUnknown).
				Where("retry_times < ?", app.Config().RetryTimes).
				Where("run_after <= ?", time.Now().Add(-runtimeDelay)).
				Limit(60000).
				Find(&queues).
				Error; err != nil {
				log.Panic(err)
			}
			log.Debug("republish queues len:", len(queues))
			if len(queues) == 0 {
				return
			}
			ch := make(chan *models.Queue)
			wg := sync.WaitGroup{}
			num := app.Config().BackConsumerGoroutineNum
			wg.Add(num)
			log.Debugf("BackConsumerGoroutineNum %d", h.Config().BackConsumerGoroutineNum)
			for i := 0; i < num; i++ {
				go func() {
					defer wg.Done()
					for {
						select {
						case <-h.Done():
							return
						case queue, ok := <-ch:
							if !ok {
								return
							}
							if h.IsClosed() {
								log.Error("republish: hub closed")
								return
							}

							switch queue.Kind {
							case amqp.ExchangeDirect:
								if producer, err = h.NewDurableNotAutoDeleteDirectProducer(queue.QueueName); err != nil {
									return
								}

								if err := producer.Publish(hub.NewMessage(queue.Data).SetRetryTimes(queue.RetryTimes + 1).SetRef(queue.UniqueId)); err != nil {
									log.Error(err)
									return
								}
							case amqp.ExchangeTopic:
								routingKey := queue.RoutingKey
								if !queue.IsTopicSelfQueue() {
									routingKey = hub.GetSelfQueueRoutingKey(queue.RoutingKey, queue.QueueName)
								}
								if producer, err = h.NewDurableNotAutoDeleteTopicProducer(queue.Exchange, routingKey); err != nil {
									return
								}

								if err := producer.Publish(hub.NewMessage(queue.Data).SetRetryTimes(queue.RetryTimes + 1).SetRef(queue.UniqueId)); err != nil {
									log.Error(err)
									return
								}
							}

							app.DB().Delete(&queue)
						}
					}
				}()
			}

			go func() {
				for _, queue := range queues {
					ch <- queue
				}
				close(ch)
			}()
			wg.Wait()
		} else {
			log.Debug("republish: Acquire Fail!")

			return
		}
	}
}
