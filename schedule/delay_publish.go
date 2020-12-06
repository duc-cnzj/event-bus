package schedule

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	dlm "github.com/DuC-cnZj/dlm"
	"mq/hub"
	"mq/models"
	"sync"
	"time"
)

func DelayPublish(h hub.Interface, lockList *sync.Map) func() {
	log.Info("delay publish job running.")
	return func() {
		lock := dlm.NewLock(app.Redis(), "delay publish", dlm.WithEX(app.Config().DLMExpiration))
		if lock.Acquire() {
			var (
				queues   []*models.DelayQueue
				producer hub.ProducerInterface
				err      error
			)

			lockList.Store(lock.GetCurrentOwner(), lock)
			defer func(t time.Time) {
				lockList.Delete(lock.GetCurrentOwner())
				lock.Release()
				if len(queues) > 0 {
					log.Infof("SUCCESS DELAY QUEUE len: %d, time is %s.", len(queues), time.Since(t).String())
				}
			}(time.Now())
			log.Debug("[SUCCESS]: delay publish")

			if err := app.DB().Where("run_after <= ?", time.Now()).Limit(10000).Find(&queues).Error; err != nil {
				log.Panic(err)
			}
			log.Debug("delay queues len:", len(queues))
			if len(queues) == 0 {
				return
			}
			ch := make(chan *models.DelayQueue)
			wg := sync.WaitGroup{}
			num := app.Config().BackConsumerGoroutineNum
			wg.Add(num)
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
								log.Error("delay publish: hub closed")
								return
							}

							switch queue.Kind {
							case amqp.ExchangeDirect:
								if producer, err = h.NewDurableNotAutoDeleteDirectProducer(queue.QueueName); err != nil {
									log.Error(err)
									return
								}

								if err := producer.Publish(hub.NewMessage(queue.Data).SetRetryTimes(queue.RetryTimes).SetRef(queue.UniqueId)); err != nil {
									log.Error(err)
									return
								}
							case amqp.ExchangeFanout:
								if producer, err = h.NewDurableNotAutoDeletePubsubProducer(queue.Exchange); err != nil {
									return
								}

								if err := producer.Publish(hub.NewMessage(queue.Data).SetRetryTimes(queue.RetryTimes).SetRef(queue.UniqueId)); err != nil {
									log.Error(err)
									return
								}
							}

							app.DB().Delete(queue)
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
			log.Debug("delay publish: Acquire Fail!")
			return
		}
	}
}
