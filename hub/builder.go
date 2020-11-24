package hub

type Prepareable interface {
	PrepareConn() error
	PrepareChannel() error
	PrepareExchange() error
	PrepareQueueDeclare() error
	PrepareQueueBind() error
}

type ProducerBuilder interface {
	Prepareable
	Build() (ProducerInterface, error)
}

type ConsumerBuilder interface {
	Prepareable
	PrepareQos() error
	PrepareDelivery() error
	Build() (ConsumerInterface, error)
}
