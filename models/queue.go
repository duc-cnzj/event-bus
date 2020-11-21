package models

import (
	"gorm.io/gorm"
	"time"
)

type Queue struct {
	ID           uint `gorm:"primarykey"`
	CreatedAt    time.Time
	UpdatedAt    time.Time
	DeletedAt    gorm.DeletedAt `gorm:"index;index:deleted_at_delay_seconds_index;index:deleted_at_retry_times_delay_seconds_index;"`
	RetryTimes   int            `json:"retry_times" gorm:"not null;default:0;index:deleted_at_retry_times_delay_seconds_index;"`
	DelaySeconds uint           `json:"delay_seconds" gorm:"not null;default:0;index:deleted_at_delay_seconds_index;index:deleted_at_retry_times_delay_seconds_index;"`

	Data      string     `json:"data" gorm:"not null;"`
	QueueName string     `json:"queue_name" gorm:"not null;"`
	Ref       int        `json:"ref" gorm:"not null;default:0;"`
	RunAfter  *time.Time `json:"run_after" gorm:"nullable;"`
}
