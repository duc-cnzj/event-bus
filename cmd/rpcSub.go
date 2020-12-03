package cmd

import (
	log "github.com/sirupsen/logrus"
	_ "net/http/pprof"
	"runtime"
	"runtime/pprof"
	"sync"

	"context"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	mq "mq/protos"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

// rpcSubCmd represents the rpcSub command
var rpcSubCmd = &cobra.Command{
	Use:   "rpcSub",
	Short: "A brief description of your command",
	PreRun: func(cmd *cobra.Command, args []string) {
		app.Boot()
		log.Infof("testMessageTotalNum %d testQueueName %s", testMessageTotalNum, testQueueName)
	},
	Run: func(cmd *cobra.Command, args []string) {
		if cpuprofile != "" {
			f, err := os.Create(cpuprofile)
			if err != nil {
				log.Fatal("could not create CPU profile: ", err)
			}
			defer f.Close() // error handling omitted for example
			if err := pprof.StartCPUProfile(f); err != nil {
				log.Fatal("could not start CPU profile: ", err)
			}
			defer pprof.StopCPUProfile()
		}
		var host = "localhost"
		if testHost != "" {
			host = testHost
		}
		dial, e := grpc.Dial(host+":"+app.Config().RpcPort, grpc.WithInsecure())
		if e != nil {
			log.Error(e)
			return
		}
		client := mq.NewMqClient(dial)
		var total int64
		now := time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wg := sync.WaitGroup{}
		num := wgnum
		wg.Add(num)
		for i := 0; i < num; i++ {
			go func() {
				defer wg.Done()
				for {
					if testMessageTotalNum > 0 && atomic.LoadInt64(&total) >= testMessageTotalNum {
						return
					}
					_, err := client.Subscribe(context.Background(), &mq.SubscribeRequest{Queue: testQueueName})
					if err != nil {
						log.Error(err)
					}
					atomic.AddInt64(&total, 1)
				}
			}()
		}

		go func() {
			wg.Wait()
			cancel()
		}()

		ch := make(chan os.Signal)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

		select {
		case <-ch:
		case <-ctx.Done():
		}
		log.Infof("消费 %d 条数据执行的时间是 %s", total, time.Since(now).String())

		log.Println("shutdown...")
		if memprofile != "" {
			f, err := os.Create(memprofile)
			if err != nil {
				log.Fatal("could not create memory profile: ", err)
			}
			defer f.Close() // error handling omitted for example
			runtime.GC()    // get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Fatal("could not write memory profile: ", err)
			}
		}
	},
}

func init() {
	testCmd.AddCommand(rpcSubCmd)
}
