package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/streadway/amqp"

	"mq/conn"
	"mq/hub"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
)

var testProducerNum int
var testQueueName string

// produceCmd represents the produce command
var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "开启一个/多个生产者推送消息",
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
		_, cancel := context.WithCancel(context.Background())
		defer cancel()
		h := hub.NewHub(mqConn, cfg, db)
		h.Config().EachQueueProducerNum = int64(testProducerNum)
		log.Infof("producer num: %d queue %s", testProducerNum, testQueueName)
		go func() {
			for {
				select {
				case <-h.Done():
					return
				default:
					producer, _ := h.ProducerManager().GetProducer(testQueueName, amqp.ExchangeDirect)
					producer.Publish(hub.Message{
						Data: fmt.Sprintf("data to test queue by %d\n", producer.GetId()),
					})
				}
			}
		}()

		ch := make(chan os.Signal)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		h.Close()
		log.Println("shutdown...")
	},
}

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().IntVarP(&testProducerNum, "producerNum", "p", 10, "--producerNum/-p 10")
	produceCmd.Flags().StringVar(&testQueueName, "queue", "test_queue", "--queue test_queue")
}
