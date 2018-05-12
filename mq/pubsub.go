package mq

import (
	"log"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

// exchange binds the publishers to the subscribers
const exchange = "pubsub"

func GetPubSub(conf Config) PubSub {
	ctx, done := context.WithCancel(context.Background())
	session := redial(ctx, conf.Url)
	return PubSub{session: session, context: ctx, done: done, route: conf.Route}
}

/*
PubSub object to wrap an instance of a connection to mq
 */
type PubSub struct {
	context context.Context
	done context.CancelFunc
	route string
	session chan chan session
}

/*
Get context of the connection to mq
 */
func (ps PubSub) GetContext() (context context.Context, done context.CancelFunc) {
	return ps.context, ps.done
}

/*
Publish publishes messages to a reconnecting session to a fanout exchange.
It receives from the application specific source of messages.
*/
func (ps PubSub) Publish(messages <-chan Message) {
	for session := range ps.session {
		var (
			running bool
			reading = messages
			pending = make(chan Message, 1)
			confirm = make(chan amqp.Confirmation, 1)
		)

		pub := <-session

		// publisher confirms for this channel/connection
		if err := pub.Confirm(false); err != nil {
			log.Printf("publisher confirms not supported")
			close(confirm) // confirms not supported, simulate by always nacking
		} else {
			pub.NotifyPublish(confirm)
		}

		log.Printf("publishing...")

	Publish:
		for {
			var msg Message
			select {
			case confirmed, ok := <-confirm:
				if !ok {
					break Publish
				}
				if !confirmed.Ack {
					log.Printf("nack message %d, body: %q", confirmed.DeliveryTag, string(msg.Content))
				}
				reading = messages

			case msg = <-pending:
				//routingKey := "ignored for fanout exchanges, application dependent for other exchanges"
				routingKey := ps.route
				err := pub.Publish(exchange, routingKey, false, false, amqp.Publishing{
					Body: msg.Content,
				})
				// Retry failed delivery on the next session
				if err != nil {
					pending <- msg
					pub.Close()
					break Publish
				}

			case msg, running = <-reading:
				// all messages consumed
				if !running {
					return
				}
				// work on pending delivery until ack'd
				pending <- msg
				reading = nil
			}
		}
	}
}

/*
Subscribe consumes deliveries from an exclusive queue from a fanout exchange and sends to the application specific messages chan.
 */
func (ps  PubSub) Subscribe(messages chan<- Message) {
	queue := identity()

	for session := range ps.session {
		sub := <-session

		if _, err := sub.QueueDeclare(queue, false, true, true, false, nil); err != nil {
			log.Printf("cannot consume from exclusive queue: %q, %v", queue, err)
			return
		}

		//routingKey := "application specific routing key for fancy toplogies"
		routingKey := ps.route
		if err := sub.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
			log.Printf("cannot consume without a binding to exchange: %q, %v", exchange, err)
			return
		}

		deliveries, err := sub.Consume(queue, "", false, true, false, false, nil)
		if err != nil {
			log.Printf("cannot consume from: %q, %v", queue, err)
			return
		}

		log.Printf("subscribed...")

		for msg := range deliveries {
			messages <- Message{msg.Body}
			sub.Ack(msg.DeliveryTag, false)
		}
	}
}

