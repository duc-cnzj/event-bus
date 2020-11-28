package bootstrapers

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"mq/config"
	"strconv"
	"strings"
)

type ConfigLoader struct {
}

func (c *ConfigLoader) Boot(app *App) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig()

	if err != nil {
		log.Fatal(err)
	}

	app.cfg = &config.Config{
		Debug:                       viper.GetBool("Debug"),
		PrintConfig:                 viper.GetBool("PrintConfig"),
		MaxJobRunningSeconds:        viper.GetUint("MaxJobRunningSeconds"),
		NackdJobNextRunDelaySeconds: viper.GetUint("NackdJobNextRunDelaySeconds"),
		RetryTimes:                  viper.GetUint("RetryTimes"),
		DLMExpiration:               viper.GetInt("DLMExpiration"),
		HttpPort:                    viper.GetString("HttpPort"),
		RpcPort:                     viper.GetString("RpcPort"),
		PrefetchCount:               viper.GetInt("PrefetchCount"),
		AmqpUrl:                     viper.GetString("AmqpUrl"),
		CronRepublishEnabled:        viper.GetBool("CronRepublishEnabled"),
		CronDelayPublishEnabled:     viper.GetBool("CronDelayPublishEnabled"),
		BackgroundConsumerEnabled:   viper.GetBool("BackgroundConsumerEnabled"),
		DBPort:                      viper.GetString("DB_PORT"),
		DBDatabase:                  viper.GetString("DB_DATABASE"),
		DBHost:                      viper.GetString("DB_HOST"),
		DBPassword:                  viper.GetString("DB_PASSWORD"),
		DBUsername:                  viper.GetString("DB_USERNAME"),
		RedisAddr:                   viper.GetString("RedisAddr"),
		RedisPassword:               viper.GetString("RedisPassword"),
		RedisUsername:               viper.GetString("RedisUsername"),
		RedisDB:                     viper.GetInt("RedisDB"),
		EachQueueConsumerNum:        viper.GetInt64("EachQueueConsumerNum"),
		EachQueueProducerNum:        viper.GetInt64("EachQueueProducerNum"),
		BackConsumerGoroutineNum:    viper.GetInt("BackConsumerGoroutineNum"),
	}
	c.printConfig(app)
	if app.cfg.Debug {
		log.SetLevel(log.DebugLevel)
		//log.SetReportCaller(true)
	}
}

func (c *ConfigLoader) printConfig(app *App) {
	cfg := app.cfg
	if !cfg.PrintConfig {
		return
	}
	l := len(getLarger(getLarger(cfg.DBHost, cfg.AmqpUrl), cfg.RedisAddr))
	f := "#%25v: %" + strconv.Itoa(-l) + "v\t#"
	padding := strings.Repeat("#", l+31)
	log.Warn(padding)
	log.Warnf(f, "Debug", cfg.Debug)
	log.Warnf(f, "PrintConfig", cfg.PrintConfig)
	log.Warnf(f, "MaxJobRunningSeconds", cfg.MaxJobRunningSeconds)
	log.Warnf(f, "NackdJobNextRunDelaySeconds", cfg.NackdJobNextRunDelaySeconds)
	log.Warnf(f, "RetryTimes", cfg.RetryTimes)
	log.Warnf(f, "DLMExpiration", cfg.DLMExpiration)
	log.Warnf(f, "HttpPort", cfg.HttpPort)
	log.Warnf(f, "RpcPort", cfg.RpcPort)
	log.Warnf(f, "PrefetchCount", cfg.PrefetchCount)
	log.Warnf(f, "AmqpUrl", cfg.AmqpUrl)
	log.Warnf(f, "CronRepublishEnabled", cfg.CronRepublishEnabled)
	log.Warnf(f, "CronDelayPublishEnabled", cfg.CronDelayPublishEnabled)
	log.Warnf(f, "BackgroundConsumerEnabled", cfg.BackgroundConsumerEnabled)
	log.Warnf(f, "DB_PORT", cfg.DBPort)
	log.Warnf(f, "DB_HOST", cfg.DBHost)
	log.Warnf(f, "DB_DATABASE", cfg.DBDatabase)
	log.Warnf(f, "DB_USERNAME", cfg.DBUsername)
	log.Warnf(f, "DB_PASSWORD", cfg.DBPassword)
	log.Warnf(f, "REDIS_ADDR", cfg.RedisAddr)
	log.Warnf(f, "REDIS_USERNAME", cfg.RedisUsername)
	log.Warnf(f, "REDIS_PASSWORD", cfg.RedisPassword)
	log.Warnf(f, "REDIS_DB", cfg.RedisDB)
	log.Warnf(f, "EachQueueConsumerNum", cfg.EachQueueConsumerNum)
	log.Warnf(f, "EachQueueProducerNum", cfg.EachQueueProducerNum)
	log.Warnf(f, "BackConsumerGoroutineNum", cfg.BackConsumerGoroutineNum)
	log.Warn(padding)
}

func getLarger(i, j string) string {
	if len(i) > len(j) {
		return i
	}

	return j
}
