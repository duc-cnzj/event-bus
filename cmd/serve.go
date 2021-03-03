package cmd

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/DuC-cnZj/event-bus/adapter"
	"github.com/DuC-cnZj/event-bus/conn"
	"github.com/DuC-cnZj/event-bus/hub"
	"github.com/DuC-cnZj/event-bus/models"
	mq "github.com/DuC-cnZj/event-bus/protos"
	"github.com/DuC-cnZj/event-bus/rpc"
	"github.com/DuC-cnZj/event-bus/schedule"
	"gorm.io/gorm"

	dlm "github.com/DuC-cnZj/dlm"
	"github.com/gofiber/fiber/v2"
	"github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/spf13/cobra"
)

var lockList sync.Map

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "run mq server",
	PreRun: func(cmd *cobra.Command, args []string) {
		app.Boot()
	},
	Run: func(cmd *cobra.Command, args []string) {
		mqConn, err := conn.NewConn(app.Config().AmqpUrl)
		if err != nil {
			log.Fatal(err)
		}
		h := hub.NewHub(mqConn, app.Config(), app.DB())

		if h.Config().BackgroundConsumerEnabled {
			h.(hub.BackgroundJobWorker).Run()
		}

		schedule := schedule.NewSchedule(h)
		schedule.Run()

		runHttp(h)

		runRpc(h)

		go func() {
			log.Info("pprof running at localhost:6060")
			log.Info(http.ListenAndServe("localhost:6060", nil))
		}()
		ch := make(chan os.Signal)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		s := <-ch
		schedule.Stop()
		log.Info("receive: ", s)

		done := make(chan struct{})
		go func() {
			h.Close()
			done <- struct{}{}
		}()

		select {
		case <-time.After(60 * time.Second):
			log.Error("timeout shutdown!(60s)")
		case <-done:
		}
		log.Info("server shutdown...")
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)
}

func runRpc(h hub.Interface) {
	listen, err := net.Listen("tcp", ":"+h.Config().RpcPort)
	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(recovery), grpc.StreamInterceptor(streamRecovery))
	mq.RegisterMqServer(server, &rpc.MQ{Hub: h})
	mq.RegisterMqTopicServer(server, &rpc.MQTopic{Hub: h})

	if h.Config().RpcPort == "" {
		log.Fatal("RpcPort required")
	}
	go func() {
		log.Infof("rpc running at %s\n", h.Config().RpcPort)
		if err := server.Serve(listen); err != nil {
			log.Fatal(err)
		}
	}()
}

func runHttp(h hub.Interface) {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	app.Get("/", func(ctx *fiber.Ctx) error {
		return ctx.JSON(struct {
			Success bool `json:"success"`
		}{
			Success: true,
		})
	})

	app.Get("/ping", func(ctx *fiber.Ctx) error {
		return ctx.SendString("pong")
	})

	app.Get("/stats", func(ctx *fiber.Ctx) error {
		h.ProducerManager().Print()
		h.ConsumerManager().Print()

		return ctx.SendString(fmt.Sprintf("Consumers: %d, Producers: %d, NumGoroutine: %d\n", h.ConsumerManager().Count(), h.ProducerManager().Count(), runtime.NumGoroutine()))
	})

	app.Get("/pub", func(ctx *fiber.Ctx) error {
		log.Debug("web pub")
		var (
			err error
			p   hub.ProducerInterface
		)
		queueName := bytes.NewBufferString(ctx.Query("queue", "test_queue")).String()

		if p, err = h.NewDurableNotAutoDeleteDirectProducer(queueName); err != nil {
			ctx.Status(fiber.StatusServiceUnavailable)

			return ctx.SendString("server unavailable")
		}
		if err := p.Publish(hub.NewMessage("pub")); err != nil {
			log.Debug("http: /pub error", err)
		}

		return ctx.JSON(struct {
			Success bool   `json:"success"`
			Queue   string `json:"queue"`
		}{
			Success: true,
			Queue:   ctx.Query("queue", "test_queue"),
		})
	})

	app.Get("/topic", func(ctx *fiber.Ctx) error {
		log.Debug("web pub")
		var (
			err error
			p   hub.ProducerInterface
		)
		topic := bytes.NewBufferString(ctx.Query("topic", "default_topic")).String()

		if p, err = h.NewDurableNotAutoDeleteTopicProducer(topic, topic); err != nil {
			ctx.Status(fiber.StatusServiceUnavailable)

			return ctx.SendString(err.Error())
		}

		if err := p.Publish(hub.NewMessage("topic: " + topic)); err != nil {
			log.Debug("http: /topic error", err)
		}

		return ctx.JSON(struct {
			Success bool   `json:"success"`
			Topic   string `json:"topic"`
		}{
			Success: true,
			Topic:   topic,
		})
	})
	app.Get("/topic_consumer", func(ctx *fiber.Ctx) error {
		log.Debug("web pub")
		var (
			err error
			p   hub.ConsumerInterface
		)
		queue := bytes.NewBufferString(ctx.Query("queue", "my_topic_queue")).String()
		topic := bytes.NewBufferString(ctx.Query("topic", "default_topic")).String()

		if p, err = h.NewDurableNotAutoDeleteTopicConsumer(queue, topic, topic); err != nil {
			ctx.Status(fiber.StatusServiceUnavailable)

			return ctx.SendString(err.Error())
		}

		if d, err := p.Consume(context.Background()); err != nil {
			log.Debug("http: /topic error", err)
			return ctx.SendString(err.Error())
		} else {
			return ctx.JSON(struct {
				Success bool   `json:"success"`
				Topic   string `json:"topic"`
				Data    string `json:"data"`
			}{
				Success: true,
				Topic:   topic,
				Data:    d.GetData(),
			})
		}

	})

	app.Get("/delay_pub", func(ctx *fiber.Ctx) error {
		log.Debug("web delay_pub")
		queueName := bytes.NewBufferString(ctx.Query("queue", "test_queue")).String()

		producer, _ := h.NewDurableNotAutoDeleteDirectProducer(queueName)

		if err := producer.DelayPublish(hub.NewMessage("delay pub").Delay(10)); err != nil {
			log.Debug("http: /pub error", err)
		}

		return ctx.JSON(struct {
			Success bool   `json:"success"`
			Queue   string `json:"queue"`
		}{
			Success: true,
			Queue:   ctx.Query("queue", "test_queue"),
		})
	})

	go func() {
		log.Infof("http server running at %s\n", h.Config().HttpPort)

		log.Fatal(app.Listen(":" + h.Config().HttpPort))
	}()
}

func runCron(h hub.Interface) *cron.Cron {
	cr := cron.New(cron.WithChain(
		cron.Recover(&adapter.CronLoggerAdapter{}),
	))

	if h.Config().CronRepublishEnabled {
		log.Info("Republish job running.")
		cr.AddFunc("@every 1s", func() {
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
				num := h.Config().BackConsumerGoroutineNum
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

									if !queue.Nackd() {
										if err := producer.Publish(hub.NewMessage(queue.Data).SetRetryTimes(queue.RetryTimes + 1).SetRef(queue.UniqueId)); err != nil {
											log.Error(err)
											return
										}
									}
								case amqp.ExchangeFanout:
									if producer, err = h.NewDurableNotAutoDeletePubsubProducer(queue.Exchange); err != nil {
										return
									}

									if !queue.Nackd() {
										if err := producer.Publish(hub.NewMessage(queue.Data).SetRetryTimes(queue.RetryTimes + 1).SetRef(queue.UniqueId)); err != nil {
											log.Error(err)
											return
										}
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
		})
	}

	if h.Config().CronDelayPublishEnabled {
		log.Info("delay publish job running.")

		cr.AddFunc("@every 1s", func() {
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
				num := h.Config().BackConsumerGoroutineNum
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
										return
									}

									if err := producer.Publish(hub.NewMessage(queue.Data).SetRetryTimes(queue.RetryTimes + 1).SetRef(queue.UniqueId)); err != nil {
										log.Error(err)
										return
									}
								case amqp.ExchangeFanout:
									if producer, err = h.NewDurableNotAutoDeletePubsubProducer(queue.Exchange); err != nil {
										return
									}

									if err := producer.Publish(hub.NewMessage(queue.Data).SetRetryTimes(queue.RetryTimes + 1).SetRef(queue.UniqueId)); err != nil {
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
		})
	}

	cr.Start()

	return cr
}

func recovery(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if e := recover(); e != nil {
			log.Errorf("[GRPC ERROR]: method: %s, error: %v", info.FullMethod, e)
			err = status.Errorf(codes.Internal, "[GRPC ERROR]: method: %s, error: %v", info.FullMethod, e)
		}
	}()

	return handler(ctx, req)
}

func streamRecovery(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if e := recover(); e != nil {
			log.Errorf("[GRPC ERROR]: method: %s error: %v", info.FullMethod, e)
			err = status.Errorf(codes.Internal, "[GRPC ERROR]: method: %s error: %v", info.FullMethod, e)
		}
	}()

	return handler(srv, ss)
}
