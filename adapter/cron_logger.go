package adapter

import (
	log "github.com/sirupsen/logrus"
)

type CronLoggerAdapter struct {
}

func (c *CronLoggerAdapter) Info(msg string, keysAndValues ...interface{}) {
	log.WithField("kv", keysAndValues).Info(msg)
}

func (c *CronLoggerAdapter) Error(err error, msg string, keysAndValues ...interface{}) {
	log.WithError(err).WithField("kv", keysAndValues).Error(msg)
}
