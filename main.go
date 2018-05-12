package main

import (
	"bufio"
	"fmt"
	"flag"
	"io"
	"os"

	"github.com/darbs/barbatos-fwk/mq"
)

/*
Read content from stdin to push to message queue
 */
func read(r io.Reader) <-chan mq.Message {
	lines := make(chan mq.Message)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(r)
		for scan.Scan() {
			lines <- mq.Message{Content:scan.Bytes()}
		}
	}()
	return lines
}

/*
Write subscriber application messages to stdout
 */
func write(w io.Writer) chan<- mq.Message {
	lines := make(chan mq.Message)
	go func() {
		for msg := range lines {
			fmt.Fprintln(w, string(msg.Content))
		}
	}()
	return lines
}

func main() {
	fmt.Printf("Fwk main start\n")
	flag.Parse()
	var mqurl = "localhost"
	var routeKey = "ATLAS_ROUTE"
	var url = flag.String("url", "amqp:///", mqurl)

	var conf = mq.Config{Url: *url, Route: routeKey}
	var ps = mq.GetPubSub(conf)
	var ctx, done = ps.GetContext()

	go func() {
		in := read(os.Stdin)
		ps.Publish(in)
		done()
	}()

	go func() {
		ps.Subscribe(write(os.Stdout))
		done()
	}()

	<-ctx.Done()
}
