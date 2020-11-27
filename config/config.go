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

	DBHost     string
	DBPort     string
	DBDatabase string
	DBUsername string
	DBPassword string

	EachQueueProducerNum int64
	EachQueueConsumerNum int64
	BackConsumerNum      int64
}
