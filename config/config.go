package config

type Config struct {
	// debug mode
	Debug bool
	// 打印config信息
	PrintConfig bool

	// 作业最大执行秒数
	MaxJobRunningSeconds uint

	// nackd queue next job run delay seconds
	NackdJobNextRunDelaySeconds uint

	// 作业最大重试次数
	RetryTimes uint
	// 分布式锁有效时间
	DLMExpiration int

	// http port
	HttpPort string

	// rpc port
	RpcPort string

	// mq dsn
	AmqpUrl       string
	PrefetchCount int

	RedisAddr     string
	RedisDB       int
	RedisUsername string
	RedisPassword string

	// 开启重新推送消息的任务
	CronRepublishEnabled bool
	// 开启延迟推送消息的任务
	CronDelayPublishEnabled bool

	// 是否开启后台 event_bus_ack_queue 和 event_bus_confirm_queue 消费队列
	BackgroundConsumerEnabled bool

	DBHost     string
	DBPort     string
	DBDatabase string
	DBUsername string
	DBPassword string

	// 每个队列的消费者数量
	EachQueueProducerNum int

	// 每个队列的生产者数量
	EachQueueConsumerNum int

	// 后台 event_bus_ack_queue 和 event_bus_confirm_queue 的协程数量
	BackConsumerGoroutineNum int
}
