package infra

import (
	confpb "big-market-kratos/internal/conf"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func NewConnection(conf *confpb.RabbitMQ) (*amqp.Connection, error) {
	connStr := fmt.Sprintf("amqp://%s:%s@%s:%d/",
		conf.Username,
		conf.Password,
		conf.Addresses,
		conf.Port,
	)
	return amqp.Dial(connStr)
}
