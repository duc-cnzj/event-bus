package models

import (
	"gorm.io/gorm"
	"time"
)

const (
	StatusUnknown = iota
	StatusAcked
	StatusNAcked
)

type Queue struct {
	ID uint `gorm:"primarykey"`

	UniqueId string `json:"unique_id" gorm:"not null;index:unique_id_idx,unique;"`

	Status       uint
	RetryTimes   int    `json:"retry_times" gorm:"default:0;"`
	Data         string `json:"data"`
	QueueName    string `json:"queue_name"`
	Ref          int    `json:"ref" gorm:"default:0;"`
	DelayQueueId int    `json:"delay_queue_id" gorm:"default:0;"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt

	DelayQueue DelayQueue `gorm:"foreignKey:delay_queue_id;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}
