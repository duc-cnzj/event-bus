package adapter

import (
	"context"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm/logger"
	"time"
)

type GormLoggerAdapter struct {
}

func (g GormLoggerAdapter) LogMode(level logger.LogLevel) logger.Interface {
	return g
}

func (g GormLoggerAdapter) Info(ctx context.Context, s string, i ...interface{}) {
	log.WithContext(ctx).WithField("data", i).Info(s)
}

func (g GormLoggerAdapter) Warn(ctx context.Context, s string, i ...interface{}) {
	log.WithContext(ctx).WithField("data", i).Warning(s)

}

func (g GormLoggerAdapter) Error(ctx context.Context, s string, i ...interface{}) {
	log.WithContext(ctx).WithField("data", i).Error(s)
}

func (g GormLoggerAdapter) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, rowsAffected := fc()

	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"beginAt":      begin,
			"rowsAffected": rowsAffected,
		}).Error(sql)
	} else {
		log.WithFields(log.Fields{
			"beginAt":      begin,
			"rowsAffected": rowsAffected,
		}).Debug(sql)
	}
}
