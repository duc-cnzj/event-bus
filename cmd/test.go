package cmd

import (
	"github.com/DuC-cnZj/event-bus/hub"
	"github.com/spf13/cobra"
)

var testMessageTotalNum int64
var testQueueName string
var kind string
var topic string
var cpuprofile string
var memprofile string
var testHost string
var wgnum int
var port string

// testCmd represents the test command
var testCmd = &cobra.Command{
	Use:   "test",
	Short: "测试专用命令",
}

func init() {
	rootCmd.AddCommand(testCmd)

	testCmd.PersistentFlags().StringVarP(&testQueueName, "queue", "q", "test_queue", "--queue test_queue")
	testCmd.PersistentFlags().Int64VarP(&testMessageTotalNum, "total", "t", 0, "--total/-t 1000000")
	testCmd.PersistentFlags().StringVarP(&kind, "kind", "k", "direct", "--kind/-k direct/fanout")
	testCmd.PersistentFlags().StringVarP(&topic, "topic", "T", hub.DefaultExchange, "--topic/-T topic")
	testCmd.PersistentFlags().StringVarP(&port, "port", "P", "", "--port/-P 9091")
	testCmd.PersistentFlags().StringVarP(&testHost, "host", "H", "", "--host/-H 127.0.0.1")

	rpcPubCmd.PersistentFlags().StringVarP(&cpuprofile, "cpu", "c", "", "--cpu/-c cpu.prof")
	rpcPubCmd.PersistentFlags().StringVarP(&memprofile, "mem", "m", "", "--mem/-m mem.prof")
	rpcPubCmd.PersistentFlags().IntVarP(&wgnum, "wgnum", "n", 30, "-n/--wgnum 30")
}
