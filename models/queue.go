package models

import (
	"gorm.io/gorm"
	"time"
)

type Status = uint8

const (
	StatusUnknown Status = iota
	StatusAcked
	StatusNacked
)

type Queue struct {
	ID uint `gorm:"primarykey"`

	Data      string `json:"data"`
	QueueName string `json:"queue_name"`

	Kind         string     `json:"kind"`
	UniqueId string `json:"unique_id" gorm:"not null;index:unique_id_idx,unique;type:string;"`

	Ref string `json:"ref" gorm:"type:string;"`

	//republish_idx
	DeletedAt   gorm.DeletedAt `gorm:"index;index:republish_idx;"`
	Status      uint8          `json:"status" gorm:"default:0;not null;index;index:republish_idx;"`
	IsConfirmed bool           `json:"is_confirmed" gorm:"index:republish_idx;"`
	RunAfter    *time.Time     `json:"run_after" gorm:"index:run_after_idx;index:republish_idx;"`
	RetryTimes  int            `json:"retry_times" gorm:"default:0;index:republish_idx;"`

	NackedAt    *time.Time `json:"nacked_at" gorm:"column:nacked_at;"`
	AckedAt     *time.Time `json:"acked_at"`
	ConfirmedAt *time.Time `json:"confirmed_at"`

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
	return q.NackedAt != nil
}

func (q *Queue) Confirmed() bool {
	return q.ConfirmedAt != nil
}
