package models

import (
	"gorm.io/gorm"
	"time"
)

type Queue struct {
	ID uint `gorm:"primarykey"`

	UniqueId string `json:"unique_id" gorm:"not null;index:unique_id_idx,unique;type:string;"`

	DeletedAt   gorm.DeletedAt `gorm:"index:republish_idx;"`
	AckedAt     *time.Time     `json:"acked_at" gorm:"index:republish_idx;"`
	NAckedAt    *time.Time     `json:"nacked_at" gorm:"column:nacked_at;index:nacked_at_idx;"`
	ConfirmedAt *time.Time     `json:"confirmed_at" gorm:"index:republish_idx;"`

	RetryTimes int    `json:"retry_times" gorm:"default:0;"`
	Data       string `json:"data"`
	QueueName  string `json:"queue_name"`

	RunAfter *time.Time `json:"run_after" gorm:"index:run_after_idx;"`
	Ref      string     `json:"ref" gorm:"index:ref_idx;type:string;"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (q *Queue) Acked() bool {
	return q.AckedAt != nil
}
func (q *Queue) Deleted() bool {
	return q.DeletedAt.Valid
}

func (q *Queue) Nackd() bool {
	return q.NAckedAt != nil
}
