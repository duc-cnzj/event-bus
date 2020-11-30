package bootstrapers

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"mq/adapter"
	"mq/models"
	"time"
)

type DBLoader struct {
}

func (D *DBLoader) Boot(app *App) {
	var (
		err error
		db  *gorm.DB
		cfg = app.cfg
	)
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
	sqlDB.SetMaxOpenConns(100)

	// SetConnMaxLifetime 设置了连接可复用的最大时间。
	sqlDB.SetConnMaxLifetime(time.Hour)

	db.AutoMigrate(&models.DelayQueue{}, &models.Queue{})

	if !cfg.Debug {
		log.Println("logger.Silent")
		db.Logger.LogMode(logger.Silent)
	} else {
		db.Logger.LogMode(logger.Info)
	}

	app.db = db
}
