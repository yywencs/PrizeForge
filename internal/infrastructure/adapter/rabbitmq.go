package adapter

import (
	"fmt"
	"prizeforge/pkg/config"

	amqp "github.com/rabbitmq/amqp091-go"
)

// NewConnection creates a RabbitMQ connection from config.
func NewConnection(cfg *config.RabbitMQConfig) (*amqp.Connection, error) {
	connStr := fmt.Sprintf("amqp://%s:%s@%s:%d/",
		cfg.Username,
		cfg.Password,
		cfg.Addresses,
		cfg.Port,
	)
	return amqp.Dial(connStr)
}
