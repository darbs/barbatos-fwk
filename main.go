package main

import (
	"bufio"
	"fmt"
	"flag"
	"io"
	"log"
	"os"
	"time"

	"github.com/darbs/barbatos-fwk/mq"
	"golang.org/x/net/context"
)

type Message struct {
	Content []byte
}

/*
Read content from stdin to push to message queue
 */
func read(r io.Reader) <-chan Message {
	lines := make(chan Message)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(r)
		for scan.Scan() {
			lines <- Message{Content: scan.Bytes()}
		}
	}()
	return lines
}

/*
Write subscriber application messages to stdout
 */
func write(w io.Writer) chan<- Message {
	lines := make(chan Message)
	go func() {
		for msg := range lines {
			fmt.Fprintln(w, string(msg.Content))
		}
	}()
	return lines
}


func main() {
	log.Println("Initializing Atlas")

	var in = read(os.Stdin)
	var mqurl = "localhost"
	//var routeKey = "ATLAS_ROUTE"
	var url = flag.String(
		"url", "amqp:///", mqurl)

	var conf = mq.Config{
		Url: *url,
		Durable: true,
		Attempts: 5,
		Delay: time.Second * 2,
		Threshold: 4,
	}
	var msgConn, err = mq.GetConnection(conf)
	if err != nil {
		fmt.Errorf("Failed to connect to message queue")
		os.Exit(1)
	}

	log.Println("Initiliazing message connection")
	ctx, cancel := context.WithCancel(context.Background())
	go msgConn.Start(ctx)

	defer func() {
		cancel()
		msgConn.Stop()
	}()

	go func() {
		msgChan, err := msgConn.Listen(
			"test_ex",
			"topic",
			"test_key",
			"consumer_test_q",
		)

		if err != nil {
			fmt.Errorf("Failed to listen to queue")
			os.Exit(1)
		}

		for{
			msg := <-msgChan
			log.Printf("msg: %v", msg)
		}
	}()

	for {
		msgConn.Publish(
			"test_ex",
			"topic",
			"test_key",
			string((<-in).Content),
		)
	}
}
