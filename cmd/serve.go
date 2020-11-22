package cmd

import (
	"context"
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

		cr := runCron(h)

		runHttp(h)

		runRpc(h)

		go func() {
			log.Warn("pprof running at localhost:6060")
			log.Warn(http.ListenAndServe("localhost:6060", nil))
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

		if _, err := h.GetAmqpConn(); err == nil {
			done := make(chan struct{})
			go func() {
				h.Close()
				done <- struct{}{}
			}()

			select {
			case <-time.After(30 * time.Second):
				log.Error("timeout shutdown!(10s)")
			case <-done:
			}
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
		log.Warnf("rpc running at %s\n", h.Config().RpcPort)
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

	app.Get("/pub", func(ctx *fiber.Ctx) error {
		log.Debug("web pub")
		var (
			err error
			p   hub.ProducerInterface
		)
		queue := ctx.Params("queue", "test_queue")

		if p, err = h.NewProducer(queue, amqp.ExchangeDirect); err != nil {
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
			Queue:   queue,
		})
	})

	go func() {
		log.Warnf("http server running at %s\n", h.Config().HttpPort)

		log.Fatal(app.Listen(":" + h.Config().HttpPort))
	}()
}

func runCron(h hub.Interface) *cron.Cron {
	cr := cron.New(cron.WithChain(
		cron.Recover(&adapter.CronLoggerAdapter{}), // or use cron.DefaultLogger
	))

	if h.Config().CronRepublishEnabled {
		log.Warn("Republish job running.")
		cr.AddFunc("@every 1m", func() {
			lock := dlm.NewLock(redisClient, "republish", dlm.WithEX(cfg.DLMExpiration))
			if lock.Acquire() {
				lockList.Store(lock.GetCurrentOwner(), lock)
				defer func() {
					lockList.Delete(lock.GetCurrentOwner())
					lock.Release()
				}()
				log.Debug("[SUCCESS]: cron republish")
				var queues []*models.Queue
				if err := db.Where("retry_times < ?", cfg.RetryTimes).Where("delay_seconds = ?", 0).Where("run_after is null OR run_after < ?", time.Now()).Limit(10000).Find(&queues).Error; err != nil {
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
						Data:       queue.Data,
						RetryTimes: queue.RetryTimes + 1,
						Ref:        int(queue.ID),
					})
					if err != nil {
						log.Panic(err)
						return
					}
					db.Delete(queue)
				}
			} else {
				log.Warning("republish: Acquire Fail!")

				return
			}
		})
	}

	if h.Config().CronDelayPushEnabled {
		log.Warn("delay push job running.")

		cr.AddFunc("@every 1s", func() {
			lock := dlm.NewLock(redisClient, "delay publish", dlm.WithEX(cfg.DLMExpiration))
			if lock.Acquire() {
				lockList.Store(lock.GetCurrentOwner(), lock)
				defer func() {
					lockList.Delete(lock.GetCurrentOwner())
					lock.Release()
				}()
				log.Debug("[SUCCESS]: delay publish")
				var queues []*models.Queue
				if err := db.Where("retry_times = 0").Where("delay_seconds > ?", 0).Where("run_after <= ? and run_after IS NOT NULL", time.Now()).Limit(10000).Find(&queues).Error; err != nil {
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
					log.Println("delay push", queue.Data, queue.QueueName)
					err := p.Publish(hub.Message{
						Data:         queue.Data,
						RetryTimes:   0,
						Ref:          int(queue.ID),
						RunAfter:     nil,
						DelaySeconds: 0,
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
