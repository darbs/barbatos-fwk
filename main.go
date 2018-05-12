package main

import (
	"bufio"
	"fmt"
	"flag"
	"io"
	"os"

	"github.com/darbs/barbatos-fwk/mq"
)

// read is this application's translation to the message format, scanning from
// stdin.
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

// write is this application's subscriber of application messages, printing to
// stdout.
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

	ctx, done := mq.GetContext()
	go func() {
		in := read(os.Stdin)
		mq.Publish(in)
		done()
	}()

	go func() {
		mq.Subscribe(write(os.Stdout))
		done()
	}()

	<-ctx.Done()
}
