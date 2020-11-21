package hub

import "time"

type MessageInterface interface {
	GetData() string
	GetRefId() int
	GetRunAfter() *time.Time
	GetDelaySeconds() uint
	IsDelay() bool
}

type Message struct {
	Data         string     `json:"data"`
	RetryTimes   int        `json:"retry_times"`
	Ref          int        `json:"ref"`
	RunAfter     *time.Time `json:"run_after"`
	DelaySeconds uint       `json:"delay_seconds"`
}

func (m *Message) GetData() string {
	return m.Data
}

func (m *Message) GetRefId() int {
	return m.Ref
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
