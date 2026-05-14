package pubsub

import (
	"bytes"
	"encoding/gob"
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
	return subscribe[T](conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		var msg T
		return msg, json.Unmarshal(data, &msg)
	})
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return subscribe[T](conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		var msg T
		b := bytes.NewBuffer(data)
		dec := gob.NewDecoder(b)
		return msg, dec.Decode(&msg)
	})
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		simpleQueueType,
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
			msg, err := unmarshaller(delivery.Body)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}

			switch handler(msg) {
			case Ack:
				err = delivery.Ack(false)
			case NackDiscard:
				err = delivery.Nack(false, false)
			case NackRequeue:
				err = delivery.Nack(false, true)
			}
			if err != nil {
				fmt.Printf("error: %v\n", err)
			}
		}
	}()

	return nil
}
