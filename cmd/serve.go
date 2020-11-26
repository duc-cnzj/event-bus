package cmd

import (
	"bytes"
	"context"
	"fmt"
	"mq/adapter"
	"mq/conn"
	"mq/hub"
	"mq/models"
	mq "mq/protos"
	"mq/rpc"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

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
		initConfig()
		LoadDB()
		LoadRedis()
	},
	Run: func(cmd *cobra.Command, args []string) {
		mqConn, err := conn.NewConn(cfg.AmqpUrl)
		if err != nil {
			log.Fatal(err)
		}
		h := hub.NewHub(mqConn, cfg, db)

		go h.ConsumeConfirmQueue()
		go h.ConsumeAckQueue()

		cr := runCron(h)

		runHttp(h)

		runRpc(h)

		go func() {
			log.Info("pprof running at localhost:6060")
			log.Info(http.ListenAndServe("localhost:6060", nil))
		}()
		ch := make(chan os.Signal)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		s := <-ch
		cr.Stop()
		lockList.Range(func(key, value interface{}) bool {
			l := value.(*dlm.Lock)
			log.Debug("Release: ", l.GetCurrentOwner())
			l.Release()
			return true
		})
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
		return ctx.SendString(fmt.Sprintf("Consumers: %d, Producers: %d, NumGoroutine: %d\n", h.ConsumerManager().Count(), h.ProducerManager().Count(), runtime.NumGoroutine()))
	})

	app.Get("/pub", func(ctx *fiber.Ctx) error {
		log.Debug("web pub")
		var (
			err error
			p   hub.ProducerInterface
		)
		bufferString := bytes.NewBufferString(ctx.Query("queue", "test_queue")).String()

		if p, err = h.NewProducer(bufferString, amqp.ExchangeDirect); err != nil {
			ctx.Status(fiber.StatusServiceUnavailable)

			return ctx.SendString("server unavailable")
		}
		if err := p.Publish(hub.Message{Data: "pub"}); err != nil {
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
			lock := dlm.NewLock(redisClient, "republish", dlm.WithEX(cfg.DLMExpiration))
			if lock.Acquire() {
				lockList.Store(lock.GetCurrentOwner(), lock)
				defer func() {
					lockList.Delete(lock.GetCurrentOwner())
					lock.Release()
				}()
				log.Debug("[SUCCESS]: cron republish")
				t := time.Now().Add(-time.Duration(h.Config().MaxJobRunningSeconds) * time.Second).String()
				var queues []*models.Queue
				if err := db.Where("retry_times < ?", cfg.RetryTimes).
					Where("nacked_at is null").
					Where("acked_at is null").
					Where("confirmed_at is not null").
					Where("created_at < ?", t).
					Limit(10000).
					Find(&queues).
					Error; err != nil {
					log.Panic(err)
				}
				log.Debug("queues len:", len(queues))
				var (
					p   hub.ProducerInterface
					err error
				)

				for _, queue := range queues {
					if h.IsClosed() {
						log.Error("republish: hub closed")
						return
					}
					if p, err = h.NewProducer(queue.QueueName, amqp.ExchangeDirect); err != nil {
						return
					}
					err := p.Publish(hub.Message{
						UniqueId:   queue.UniqueId,
						Data:       queue.Data,
						RetryTimes: queue.RetryTimes + 1,
						Ref:        int(queue.ID),
					})
					if err != nil {
						log.Panic(err)
						return
					}
				}
			} else {
				log.Warning("republish: Acquire Fail!")

				return
			}
		})
	}

	if h.Config().CronDelayPushEnabled {
		log.Info("delay push job running.")

		cr.AddFunc("@every 1s", func() {
			lock := dlm.NewLock(redisClient, "delay publish", dlm.WithEX(cfg.DLMExpiration))
			if lock.Acquire() {
				lockList.Store(lock.GetCurrentOwner(), lock)
				defer func() {
					lockList.Delete(lock.GetCurrentOwner())
					lock.Release()
				}()
				log.Debug("[SUCCESS]: delay publish")
				var queues []*models.DelayQueue
				if err := db.Where("run_after <= ?", time.Now()).Limit(10000).Find(&queues).Error; err != nil {
					log.Panic(err)
				}
				log.Debug("delay queues len:", len(queues))
				var (
					p   hub.ProducerInterface
					err error
				)

				for _, queue := range queues {
					if h.IsClosed() {
						log.Error("delay publish: hub closed")
						return
					}
					if p, err = h.NewProducer(queue.QueueName, amqp.ExchangeDirect); err != nil {
						return
					}
					err := p.Publish(hub.Message{
						QueueName: queue.QueueName,
						UniqueId:  queue.UniqueId,
						Data:      queue.Data,
					})
					if err != nil {
						log.Panic("delay publish error", err)
						return
					}
					db.Delete(queue)
				}
			} else {
				log.Warning("delay publish: Acquire Fail!")
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
