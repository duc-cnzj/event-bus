package models

import (
	"gorm.io/gorm"
	"time"
)

type DelayQueue struct {
	ID uint `gorm:"primarykey"`

	UniqueId     string     `json:"unique_id" gorm:"not null;index:unique_id_idx,unique;type:varchar(100);"`
	Data         string     `json:"data" gorm:"not null;"`
	QueueName    string     `json:"queue_name" gorm:"not null;type:varchar(50);"`
	RunAfter     *time.Time `json:"run_after" gorm:"nullable;index:run_after_deleted_at_idx;"`
	DelaySeconds uint       `json:"delay_seconds" gorm:"not null;default:0;"`
	Ref          string     `json:"ref" gorm:"index:ref_idx;type:varchar(100);"`
	RetryTimes   uint8      `json:"retry_times" gorm:"default:0;"`
	Kind         string     `json:"kind" gorm:"type:varchar(100);"`
	Exchange     string     `json:"exchange" gorm:"not null;type:varchar(50);"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index;index:run_after_deleted_at_idx;"`
}
