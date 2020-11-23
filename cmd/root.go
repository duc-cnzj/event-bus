package cmd

import (
	"fmt"
	"mq/adapter"
	"mq/config"
	"mq/models"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var cfg *config.Config
var redisClient *redis.Client
var db *gorm.DB

var rootCmd = &cobra.Command{
	Use:   "app",
	Short: "mq event bus",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initLogger)
}

func initLogger() {
	log.SetLevel(log.InfoLevel)
	fmt.Print(`
          _____                   _______         
         /\    \                 /::\    \        
        /::\____\               /::::\    \       
       /::::|   |              /::::::\    \      
      /:::::|   |             /::::::::\    \     
     /::::::|   |            /:::/~~\:::\    \    
    /:::/|::|   |           /:::/    \:::\    \   
   /:::/ |::|   |          /:::/    / \:::\    \  
  /:::/  |::|___|______   /:::/____/   \:::\____\ 
 /:::/   |::::::::\    \ |:::|    |     |:::|    |
/:::/    |:::::::::\____\|:::|____|     |:::|____|
\::/    / ~~~~~/:::/    / \:::\   _\___/:::/    / 
 \/____/      /:::/    /   \:::\ |::| /:::/    /  
             /:::/    /     \:::\|::|/:::/    /   
            /:::/    /       \::::::::::/    /    
           /:::/    /         \::::::::/    /     
          /:::/    /           \::::::/    /      
         /:::/    /             \::::/____/       
        /:::/    /               |::|    |        
        \::/    /                |::|____|        
         \/____/                  ~~		@2020.11 by duc.
`)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
}

func initConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal(err)
	}
	cfg = &config.Config{
		Debug:                viper.GetBool("Debug"),
		MaxJobRunningSeconds: viper.GetUint("MaxJobRunningSeconds"),
		RetryTimes:           viper.GetUint("RetryTimes"),
		DLMExpiration:        viper.GetInt("DLMExpiration"),
		HttpPort:             viper.GetString("HttpPort"),
		RpcPort:              viper.GetString("RpcPort"),
		PrefetchCount:        viper.GetInt("PrefetchCount"),
		AmqpUrl:              viper.GetString("AmqpUrl"),
		CronRepublishEnabled: viper.GetBool("CronRepublishEnabled"),
		CronDelayPushEnabled: viper.GetBool("CronDelayPushEnabled"),
		DBPort:               viper.GetString("DB_PORT"),
		DBDatabase:           viper.GetString("DB_DATABASE"),
		DBHost:               viper.GetString("DB_HOST"),
		DBPassword:           viper.GetString("DB_PASSWORD"),
		DBUsername:           viper.GetString("DB_USERNAME"),
		RedisAddr:            viper.GetString("RedisAddr"),
		RedisPassword:        viper.GetString("RedisPassword"),
		RedisUsername:        viper.GetString("RedisUsername"),
		RedisDB:              viper.GetInt("RedisDB"),
	}
	l := len(getLarger(getLarger(cfg.DBHost, cfg.AmqpUrl), cfg.RedisAddr))
	f := "#%25v: %" + strconv.Itoa(-l) + "v\t#"
	padding := strings.Repeat("#", l+31)
	log.Warn(padding)
	log.Warnf(f, "Debug", cfg.Debug)
	log.Warnf(f, "MaxJobRunningSeconds", cfg.MaxJobRunningSeconds)
	log.Warnf(f, "RetryTimes", cfg.RetryTimes)
	log.Warnf(f, "DLMExpiration", cfg.DLMExpiration)
	log.Warnf(f, "HttpPort", cfg.HttpPort)
	log.Warnf(f, "RpcPort", cfg.RpcPort)
	log.Warnf(f, "PrefetchCount", cfg.PrefetchCount)
	log.Warnf(f, "AmqpUrl", cfg.AmqpUrl)
	log.Warnf(f, "CronRepublishEnabled", cfg.CronRepublishEnabled)
	log.Warnf(f, "CronDelayPushEnabled", cfg.CronDelayPushEnabled)
	log.Warnf(f, "DB_PORT", cfg.DBPort)
	log.Warnf(f, "DB_HOST", cfg.DBHost)
	log.Warnf(f, "DB_DATABASE", cfg.DBDatabase)
	log.Warnf(f, "DB_USERNAME", cfg.DBUsername)
	log.Warnf(f, "DB_PASSWORD", cfg.DBPassword)
	log.Warnf(f, "REDIS_ADDR", cfg.RedisAddr)
	log.Warnf(f, "REDIS_USERNAME", cfg.RedisUsername)
	log.Warnf(f, "REDIS_PASSWORD", cfg.RedisPassword)
	log.Warnf(f, "REDIS_DB", cfg.RedisDB)
	log.Warn(padding)
	if cfg.Debug {
		log.SetLevel(log.DebugLevel)
	}
}

func getLarger(i, j string) string {
	if len(i) > len(j) {
		return i
	}

	return j
}

func LoadRedis() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Username: cfg.RedisUsername,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
}

func LoadDB() {
	var err error
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", cfg.DBUsername, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBDatabase)
	log.Debug("mysql dsn: ", dsn)
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
		PrepareStmt: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	sqlDB, err := db.DB()

	db.Logger = &adapter.GormLoggerAdapter{}

	// SetMaxIdleConns 设置空闲连接池中连接的最大数量
	sqlDB.SetMaxIdleConns(10)

	// SetMaxOpenConns 设置打开数据库连接的最大数量。
	sqlDB.SetMaxOpenConns(1000)

	// SetConnMaxLifetime 设置了连接可复用的最大时间。
	sqlDB.SetConnMaxLifetime(time.Hour)

	db.AutoMigrate(&models.Queue{})
}
