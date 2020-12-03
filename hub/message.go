package hub

import "time"

type MessageInterface interface {
	GetData() string
	GetRunAfter() *time.Time
	GetDelaySeconds() uint
	IsDelay() bool
	GetUniqueId() string
}

type Message struct {
	UniqueId     string
	Kind         string
	Data         string     `json:"data"`
	RetryTimes   int        `json:"retry_times"`
	Ref          string     `json:"ref"`
	RunAfter     *time.Time `json:"run_after"`
	DelaySeconds uint       `json:"delay_seconds"`
	QueueName    string     `json:"queue_name"`
	AckedAt      time.Time  `json:"acked_at"`
}

func (m *Message) GetUniqueId() string {
	return m.UniqueId
}

func (m *Message) GetData() string {
	return m.Data
}

func (m *Message) GetRunAfter() *time.Time {
	return m.RunAfter
}

func (m *Message) GetDelaySeconds() uint {
	return m.DelaySeconds
}

func (m *Message) IsDelay() bool {
	return m.DelaySeconds > 0
}
