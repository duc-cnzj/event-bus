package models

import (
	"gorm.io/gorm"
	"time"
)

type Queue struct {
	ID uint `gorm:"primarykey"`

	UniqueId string `json:"unique_id" gorm:"not null;index:unique_id_idx,unique;type:string;"`

	AckedAt     *time.Time `json:"acked_at"`
	NAckedAt    *time.Time `json:"nacked_at" gorm:"column:nacked_at;"`
	ConfirmedAt *time.Time `json:"confirmed_at"`

	RetryTimes int    `json:"retry_times" gorm:"default:0;"`
	Data       string `json:"data"`
	QueueName  string `json:"queue_name"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt
}

func (q *Queue) Acked() bool {
	return q.AckedAt != nil
}

func (q *Queue) NAcked() bool {
	return q.NAckedAt != nil
}
