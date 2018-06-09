package messenger

import (
	"fmt"
	"log"
	"time"
	"context"

	"github.com/rafaeljesus/rabbus"
)

type Connection struct {
	rabbit *rabbus.Rabbus
}

func connectionStateChange (name, from, to string) {
	log.Println("Connection state %v changed from %v to %v", name, from, to)
}

func GetConnection(config Config) (Connection, error) {
	rab, err := rabbus.New(
		config.Url,
		rabbus.Durable(config.Durable),
		rabbus.Attempts(config.Attempts),
		rabbus.Sleep(config.Delay),
		rabbus.Threshold(config.Threshold),
		rabbus.OnStateChange(connectionStateChange),
	)

	if err == nil {
		return Connection{rab}, nil
	}

	return Connection{}, err
}

func (c Connection) Start(ctx context.Context) {
	c.rabbit.Run(ctx)
}

func (c Connection) Stop() {
	c.rabbit.Close()
}

func (c Connection) Listen(exchange string, kind string, key string, queue string) (chan Message, error) {
	msgChan := make(chan Message)
	messages, err := c.rabbit.Listen(rabbus.ListenConfig{
		Exchange: exchange,
		Kind:     kind,
		Key:      key,
		Queue:    queue,
	})
	if err != nil {
		log.Fatalf("Failed to create listener %s", err)
		return msgChan, err
	}

	// TODO context this shit
	go func() {
		for {
			log.Println("Listening for messages...")

			m, ok := <-messages

			if !ok {
				log.Println("Stop listening messages!")
				// TODO log failure
				//return msgChan, fmt.Errorf("error recieving messge")
			}

			m.Ack(false) // todo configurable

			//log.Println(string(m.Body))
			//log.Println("Message was consumed")

			msgChan <- Message{string(m.Body)}
		}
	}()

	return msgChan, nil
}

func (c Connection) Publish(exchange string, kind string, key string, payload []byte) error {
	msg := rabbus.Message{
		Exchange: exchange,
		Kind:     kind,
		Key:      key,
		Payload:  []byte(payload),
	}

	c.rabbit.EmitAsync() <- msg

	select {
	case <-c.rabbit.EmitOk():
	case err := <-c.rabbit.EmitErr():
		return err
	case <-time.After(time.Second * 3):
		return fmt.Errorf("Failed")
	}

	return nil
}
