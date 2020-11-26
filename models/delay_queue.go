package models

import (
	"gorm.io/gorm"
	"time"
)

type DelayQueue struct {
	ID uint `gorm:"primarykey"`

	UniqueId     string     `json:"unique_id" gorm:"not null;index:unique_id_idx,unique;"`
	Data         string     `json:"data" gorm:"not null;"`
	QueueName    string     `json:"queue_name" gorm:"not null;"`
	RunAfter     *time.Time `json:"run_after" gorm:"nullable;"`
	DelaySeconds uint       `json:"delay_seconds" gorm:"not null;default:0;"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt
}
