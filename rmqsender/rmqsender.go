package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/streadway/amqp"
)

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

func main() {
	var mode Mode
	if len(os.Args) != 6 {
		fmt.Println("Usage: " + os.Args[0] + " [-q <queue name> | -e <exchange name>] <content type> <value type> <value>")
		fmt.Println("")
		fmt.Println("Examples:")
		fmt.Println(os.Args[0] + " -q log text/plain string Hello")
		fmt.Println(os.Args[0] + " -q data_feed application/bytes int 42")
		fmt.Println(os.Args[0] + " -e data_feed application/bytes int 42")
		return
	}

	modeStr := os.Args[1]
	name := os.Args[2]
	contentType := os.Args[3]
	valueType := os.Args[4]
	value := os.Args[5]
	if modeStr == "-e" {
		mode = modeExchange
	} else if modeStr == "-q" {
		mode = modeQueue
	} else {
		log.Println("Invalid mode: " + modeStr)
		return
	}
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	var exchange string
	var routingKey string

	if mode == modeExchange {
		exchange = name
		routingKey = ""

		err = ch.ExchangeDeclare(
			exchange, // name
			"fanout", // type
			true,     // durable
			false,    // auto-deleted
			false,    // internal
			false,    // no-wait
			nil,      // arguments
		)
		failOnError(err, "Failed to create exchange")

	} else if mode == modeQueue {
		exchange = ""
		routingKey = name

		_, err := ch.QueueDeclare(
			routingKey, // name
			false,      // durable
			false,      // delete when unused
			false,      // exclusive
			false,      // no-wait
			nil,        // arguments
		)
		failOnError(err, "Failed to declare a queue")

	}

	//Setup buffer and Publish
	var buf []byte
	if valueType == "string" {
		buf = []byte(value)
	} else if valueType == "int" {
		v, err := strconv.Atoi(value)
		if err != nil {
			fmt.Println(err)
			return
		}
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(v))
	}
	err = ch.Publish(
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: contentType,
			Body:        buf,
		})
	failOnError(err, "Failed to publish a message")

	if mode == modeQueue {
		log.Printf(" [x] Sent %s as %s with type %s in queue #%s", value, contentType, valueType, routingKey)
	} else if mode == modeExchange {
		log.Printf(" [x] Published %s as %s with type %s to on exchange #%s", value, contentType, valueType, exchange)
	}
}
