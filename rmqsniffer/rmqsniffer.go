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

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {

	if len(os.Args) != 2 {
		fmt.Println("Usage:" + os.Args[0] + " <chan name>")
		return
	}
	chanName := os.Args[1]

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

	q, err := ch.QueueDeclare(
		chanName, // name
		false,    // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

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
			fmt.Printf("%s | %s | %s\n", time.Now(), chanName, d.ContentType)
			switch d.ContentType {
			case "text/plain":
				fmt.Println(d.Body)
			default:
				fmt.Println(hex.Dump(d.Body))
			}
		}
	}()

	fmt.Printf("Listening for messages on %s\n", chanName)

	for {
		if killed == true {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	ch.Close()
	conn.Close()
}
