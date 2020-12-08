package hub

import (
	"time"
)

type MessageInterface interface {
	SetExchange(exchange string) MessageInterface
	SetRunAfter(runAfter *time.Time) MessageInterface
	SetRetryTimes(times uint8) MessageInterface
	SetRoutingKey(routingKey string) MessageInterface
	SetQueueName(queueName string) MessageInterface
	SetKind(kind string) MessageInterface
	SetUniqueIdIfNotExist(uniqueId string) MessageInterface
	SetUniqueId(uniqueId string) MessageInterface
	SetRef(ref string) MessageInterface
	Delay(seconds uint) MessageInterface
	GetQueueName() string
	GetExchange() string
	GetKind() string
	GetUniqueId() string
	GetData() string
	GetRunAfter() *time.Time
	GetRef() string
	GetRoutingKey() string
	GetRetryTimes() uint8
	GetDelaySeconds() int64
	IsDelay() bool
}

type ConfirmMessage struct {
	UniqueId   string
	Data       string
	QueueName  string
	Kind       string
	Exchange   string
	RoutingKey string
	Ref        string
	RunAfter   *time.Time
	RetryTimes uint8
}

func NewConfirmMessage(uniqueId string, data string, queueName string, kind string, exchange string, ref string, runAfter *time.Time, retryTimes uint8, routingKey string) *ConfirmMessage {
	return &ConfirmMessage{UniqueId: uniqueId, Data: data, QueueName: queueName, Kind: kind, Exchange: exchange, Ref: ref, RunAfter: runAfter, RetryTimes: retryTimes, RoutingKey: routingKey}
}

type AckMessage struct {
	UniqueId string
	AckedAt  time.Time
}

func NewAckMessage(uniqueId string, ackedAt time.Time) *AckMessage {
	return &AckMessage{UniqueId: uniqueId, AckedAt: ackedAt}
}

type MetaData struct {
	// 元数据
	QueueName  string
	Kind       string
	Exchange   string
	RoutingKey string
}

type Message struct {
	UniqueId string
	Data     string

	MetaData   MetaData
	RetryTimes uint8
	Ref        string
	RunAfter   *time.Time

	// 延迟队列数据
	DelaySeconds time.Duration
}

func NewMessage(data string) *Message {
	return &Message{Data: data}
}

func (m *Message) SetExchange(exchange string) MessageInterface {
	m.MetaData.Exchange = exchange

	return m
}
func (m *Message) SetRunAfter(runAfter *time.Time) MessageInterface {
	m.RunAfter = runAfter

	return m
}
func (m *Message) SetRetryTimes(times uint8) MessageInterface {
	m.RetryTimes = times

	return m
}
func (m *Message) SetQueueName(queueName string) MessageInterface {
	m.MetaData.QueueName = queueName

	return m
}
func (m *Message) SetKind(kind string) MessageInterface {
	m.MetaData.Kind = kind

	return m
}
func (m *Message) SetUniqueIdIfNotExist(uniqueId string) MessageInterface {
	if m.UniqueId == "" {
		m.UniqueId = uniqueId
	}

	return m
}
func (m *Message) SetUniqueId(uniqueId string) MessageInterface {
	m.UniqueId = uniqueId

	return m
}
func (m *Message) SetRoutingKey(routingKey string) MessageInterface {
	m.MetaData.RoutingKey = routingKey

	return m
}
func (m *Message) SetRef(ref string) MessageInterface {
	m.Ref = ref

	return m
}
func (m *Message) Delay(seconds uint) MessageInterface {
	m.DelaySeconds = time.Duration(seconds) * time.Second

	return m
}

func (m *Message) GetQueueName() string {
	return m.MetaData.QueueName
}
func (m *Message) GetExchange() string {
	return m.MetaData.Exchange
}
func (m *Message) GetKind() string {
	return m.MetaData.Kind
}
func (m *Message) GetRoutingKey() string {
	return m.MetaData.RoutingKey
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
func (m *Message) GetRef() string {
	return m.Ref
}
func (m *Message) GetRetryTimes() uint8 {
	return m.RetryTimes
}
func (m *Message) GetDelaySeconds() int64 {
	return int64(m.DelaySeconds.Seconds())
}
func (m *Message) IsDelay() bool {
	return m.DelaySeconds.Seconds() > 0
}
