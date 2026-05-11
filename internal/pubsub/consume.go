package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

const (
	Ack Acktype = iota
	NackDiscard
	NackRequeue
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %v", err)
	}

	deliveries, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	go func() {
		defer ch.Close()
		for delivery := range deliveries {
			var msg T
			err = json.Unmarshal(delivery.Body, &msg)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}
			switch handler(msg) {
			case Ack:
				err = delivery.Ack(false)
				fmt.Println("ackType: Ack")
			case NackDiscard:
				err = delivery.Nack(false, false)
				fmt.Println("ackType: NackDiscard")
			case NackRequeue:
				err = delivery.Nack(false, true)
				fmt.Println("ackType: NackRequeue")
			}
			if err != nil {
				fmt.Printf("error: %v\n", err)
			}
		}
	}()

	return nil
}
