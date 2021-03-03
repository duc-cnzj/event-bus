package models

import (
	"strings"
	"time"

	"github.com/streadway/amqp"
	"gorm.io/gorm"
)

type Status = uint8

const (
	StatusUnknown Status = iota
	StatusAcked
	StatusNacked
)

type Queue struct {
	ID uint `gorm:"primarykey"`

	UniqueId string `json:"unique_id" gorm:"not null;index:unique_id_idx,unique;type:varchar(100);"`
	Data     string `json:"data"`

	QueueName  string `json:"queue_name" gorm:"type:varchar(50);"`
	Kind       string `json:"kind" gorm:"type:varchar(20);"`
	Exchange   string `json:"exchange" gorm:"type:varchar(100);"`
	RoutingKey string `json:"routing_key" gorm:"type:varchar(50);"`

	Ref string `json:"ref" gorm:"type:varchar(100);"`

	//republish_idx
	DeletedAt   gorm.DeletedAt `gorm:"index;index:republish_idx;"`
	Status      uint8          `json:"status" gorm:"default:0;not null;index;index:republish_idx;"`
	IsConfirmed bool           `json:"is_confirmed" gorm:"index:republish_idx;"`
	RunAfter    *time.Time     `json:"run_after" gorm:"index:run_after_idx;index:republish_idx;"`
	RetryTimes  uint8          `json:"retry_times" gorm:"default:0;index:republish_idx;"`

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

func (q *Queue) IsTopicSelfQueue() bool {
	return q.Kind == amqp.ExchangeTopic && strings.Contains(q.RoutingKey, "@@@")
}
