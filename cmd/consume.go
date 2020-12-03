package cmd

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"mq/conn"
	"mq/hub"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
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

		var total int64

		now := time.Now()
		h := hub.NewHub(mqConn, app.Config(), app.DB())
		h.Config().EachQueueConsumerNum = testConsumerNum
		log.Infof("consumer num: %d queue %s", testConsumerNum, testQueueName)

		wg := sync.WaitGroup{}
		wg.Add(testConsumerNum)
		for i := 0; i < testConsumerNum; i++ {
			consumer, _ := h.ConsumerManager().GetDurableNotAutoDeleteConsumer(testQueueName, kind, topic)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-h.Done():
					default:
						if testMessageTotalNum > 0 && atomic.LoadInt64(&total) >= testMessageTotalNum {
							log.Info(atomic.LoadInt64(&total))
							return
						}
						atomic.AddInt64(&total, 1)
						if consume, err := consumer.Consume(context.Background()); err != nil {
							atomic.AddInt64(&total, -1)
							log.Error(err)
							return
						} else {
							if err := consumer.Ack(consume.UniqueId); err != nil {
								atomic.AddInt64(&total, -1)
								log.Error(err)
							}
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
		log.Infof("消费 %d 条数据执行的时间是 %s", total, time.Since(now).String())
		h.Close()
		log.Println("shutdown...")
	},
}

func init() {
	testCmd.AddCommand(consumeCmd)

	consumeCmd.Flags().IntVarP(&testConsumerNum, "consumerNum", "c", 10, "--consumerNum/-c 10")
}
