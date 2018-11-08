package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/streadway/amqp"
)

var killed = false

type Mode int

const (
	modeQueue    Mode = iota
	modeExchange      = iota
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func saveJPEGFrame(body []byte) {
	f, err := os.Create(fmt.Sprintf("frame_%s.jpg", time.Now().String()))
	if err != nil {
		return
	}
	f.Write(body)
	f.Close()
}

func main() {
	var mode Mode

	if len(os.Args) != 3 {
		fmt.Println("Usage:" + os.Args[0] + " [-q <queue name> | -e <exchange name>]")
		return
	}
	modeStr := os.Args[1]
	name := os.Args[2]

	if modeStr == "-e" {
		mode = modeExchange
	} else if modeStr == "-q" {
		mode = modeQueue
	} else {
		log.Println("Invalid mode: " + modeStr)
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			fmt.Println(sig)
			killed = true
		}
	}()

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	var q amqp.Queue
	if mode == modeExchange {
		//Create the exchange
		err = ch.ExchangeDeclare(
			name,     // name
			"fanout", // type
			true,     // durable
			false,    // auto-deleted
			false,    // internal
			false,    // no-wait
			nil,      // arguments
		)
		failOnError(err, "Failed to declare an exchange")
		//Create the queue with a random name
		q, err = ch.QueueDeclare(
			"",    // name
			false, // durable
			false, // delete when usused
			true,  // exclusive
			false, // no-wait
			nil,   // arguments
		)
		failOnError(err, "Failed to declare a queue")
		//Bind this queue to this exchange so that exchange will publish here
		err = ch.QueueBind(
			q.Name, // queue name
			"",     // routing key
			name,   // exchange
			false,
			nil)
		failOnError(err, "Failed to bind a queue")
	} else if mode == modeQueue {

		q, err = ch.QueueDeclare(
			name,  // name
			false, // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		failOnError(err, "Failed to declare a queue")
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			fmt.Printf("%s: #%s <%s>\n", time.Now(), name, d.ContentType)
			switch d.ContentType {
			case "text/plain":
				fmt.Println(d.Body)
			case "image/jpeg":
				saveJPEGFrame(d.Body)
			default:
				fmt.Println(hex.Dump(d.Body))
			}
		}
	}()

	fmt.Printf("Listening for messages on %s\n", name)

	for {
		if killed == true {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	ch.Close()
	conn.Close()
}
