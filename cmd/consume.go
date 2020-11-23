package cmd

import (
	"context"
	"mq/conn"
	"mq/hub"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/spf13/cobra"
)

var testConsumerNum int

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "开启一个/多个消费者消费",
	PreRun: func(cmd *cobra.Command, args []string) {
		if testProducerNum <= 0 {
			log.Error("error num.")
			os.Exit(1)
		}
		initConfig()
		LoadDB()
		LoadRedis()
	},
	Run: func(cmd *cobra.Command, args []string) {
		mqConn, err := conn.NewConn(cfg.AmqpUrl)
		if err != nil {
			log.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer func() {
			cancel()
			log.Warn("ctx canceled")
		}()

		h := hub.NewHub(mqConn, cfg, db)
		log.Info("consumer num: ", testConsumerNum)
		for i := 0; i < testConsumerNum; i++ {
			go func(i int) {
				consumer, err := h.ConsumerManager().GetConsumer(testQueueName, amqp.ExchangeDirect)
				if err != nil {
					log.Fatal(err)
				}
				defer consumer.Close()
				go func() {
					for {
						consumer.Consume(ctx)
					}
				}()
				select {
				case <-ctx.Done():
				case <-h.AmqpConnDone():
				}
				log.Infof("consumer %d exit", i)
			}(i)
		}

		ch := make(chan os.Signal)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		h.Close()
		log.Println("shutdown...")
	},
}

func init() {
	rootCmd.AddCommand(consumeCmd)

	consumeCmd.Flags().IntVarP(&testConsumerNum, "consumerNum", "c", 10, "--consumerNum/-c 10")
	consumeCmd.Flags().StringVar(&testQueueName, "queue", "test_queue", "--queue test_queue")
}
