package cmd

import "github.com/spf13/cobra"

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"mq/conn"
	"mq/hub"
	"os"
	"os/signal"
	"syscall"
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
		ctx, cancel := context.WithCancel(context.Background())
		h := hub.NewHub(mqConn, cfg, db)
		log.Info("producer num: ", testProducerNum)
		for i := 0; i < testProducerNum; i++ {
			go func(i int) {
				producer, err := h.ProducerManager().GetProducer("test", amqp.ExchangeDirect)
				if err != nil {
					log.Fatal(err)
				}
				defer producer.Close()
				go func() {
					for {
						producer.Publish(hub.Message{
							Data: fmt.Sprintf("data to test queue by %d\n", i),
						})
					}
				}()
				select {
				case <-ctx.Done():
				case <-h.AmqpConnDone():
				}
				log.Infof("producer %d exit", i)
			}(i)
		}

		ch := make(chan os.Signal)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		h.Close()
		cancel()
		log.Println("shutdown...")
	},
}

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().IntVarP(&testProducerNum, "producerNum", "p", 10, "--producerNum/-p 10")
	produceCmd.Flags().StringVar(&testQueueName, "queue", "test_queue", "--queue test_queue")
}
