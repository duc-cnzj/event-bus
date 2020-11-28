package hub

import (
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrorServerUnavailable = status.Errorf(codes.Unavailable, "server unavailable")
var ErrorAmqpConnClosed = errors.New("amqp conn closed")
var ErrorAlreadyAcked = errors.New("already acked")

var DefaultExchange = "event_bus_default_exchange"
var AckQueueName = "event_bus_ack_queue"
var ConfirmQueueName = "event_bus_confirm_queue"
var DelayQueueName = "event_bus_delay_queue"
