package cmd

import (
	"context"
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
		if testConsumerNum <= 0 {
			log.Errorf("error num %d.", testConsumerNum)
			os.Exit(1)
		}
		app.Boot()
	},
	Run: func(cmd *cobra.Command, args []string) {
		mqConn, err := conn.NewConn(app.Config().AmqpUrl)
		if err != nil {
			log.Fatal(err)
		}

		h := hub.NewHub(mqConn, app.Config(), app.DB())
		h.Config().EachQueueConsumerNum = testConsumerNum
		log.Infof("consumer num: %d queue %s", testConsumerNum, testQueueName)

		for i := 0; i < testConsumerNum; i++ {
			consumer, _ := h.ConsumerManager().GetConsumer(testQueueName, amqp.ExchangeDirect)
			go func() {
				for {
					if consume, err := consumer.Consume(context.Background()); err != nil {
						log.Error(err)
						return
					} else {
						if err := consumer.Ack(consume.UniqueId); err != nil {
							log.Error(err)
						}
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
