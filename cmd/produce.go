package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/streadway/amqp"
	"sync"
	"sync/atomic"
	"time"

	"mq/conn"
	"mq/hub"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
)

var testProducerNum int

// produceCmd represents the produce command
var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "开启一个/多个生产者推送消息",
	PreRun: func(cmd *cobra.Command, args []string) {
		if testProducerNum <= 0 {
			log.Error("error num.")
			os.Exit(1)
		}
		app.Boot()
	},
	Run: func(cmd *cobra.Command, args []string) {
		var total int64

		now := time.Now()

		mqConn, err := conn.NewConn(app.Config().AmqpUrl)
		if err != nil {
			log.Fatal(err)
		}
		_, cancel := context.WithCancel(context.Background())
		defer cancel()
		h := hub.NewHub(mqConn, app.Config(), app.DB())
		h.Config().EachQueueProducerNum = testProducerNum
		log.Infof("producer num: %d queue %s", testProducerNum, testQueueName)

		wg := sync.WaitGroup{}
		wg.Add(testProducerNum)
		for i := 0; i < testProducerNum; i++ {
			producer, _ := h.ProducerManager().GetProducer(testQueueName, amqp.ExchangeDirect)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-h.Done():
						return
					default:
						if testMessageTotalNum > 0 && atomic.LoadInt64(&total) >= testMessageTotalNum {
							log.Info(atomic.LoadInt64(&total))
							return
						}
						atomic.AddInt64(&total, 1)
						if err = producer.Publish(hub.Message{
							Data: fmt.Sprintf("data to test queue by %d\n", producer.GetId()),
						}); err != nil {
							log.Error(err)
							atomic.AddInt64(&total, -1)
						}
					}
				}
			}()
		}

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			wg.Wait()
			cancel()
		}()

		ch := make(chan os.Signal)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

		select {
		case <-ctx.Done():
		case <-ch:
		}
		log.Infof("生产 %d 条数据执行的时间是 %s", total, time.Since(now).String())

		h.Close()
		log.Println("shutdown...")
	},
}

func init() {
	testCmd.AddCommand(produceCmd)

	produceCmd.Flags().IntVarP(&testProducerNum, "producerNum", "p", 10, "--producerNum/-p 10")
}
