package cmd

import (
	"context"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/streadway/amqp"
	"mq/conn"
	"mq/hub"
	"os"
	"os/signal"
	"syscall"
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
		h.Config().EachQueueConsumerNum = int64(testConsumerNum)
		log.Infof("consumer num: %d queue %s", testConsumerNum, testQueueName)

		for i := 0; i < testConsumerNum; i++ {

			consumer, _ := h.ConsumerManager().GetConsumer(testQueueName, amqp.ExchangeDirect)

			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case <-h.Done():
						return
					case d, ok := <-consumer.Delivery():
						if !ok {
							return
						}
						var msg hub.Message
						json.Unmarshal(d.Body, &msg)
						if err := consumer.Ack(msg.UniqueId); err != nil {
							log.Error(err)
						}
						d.Ack(false)
					}
				}
			}()
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
