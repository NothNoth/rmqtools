package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	if len(os.Args) != 5 {
		fmt.Println("Usage: " + os.Args[0] + " <chan name> <content type> <value type> <value>")
		fmt.Println("")
		fmt.Println("Examples:")
		fmt.Println(os.Args[0] + " log_chan text/plain string Hello")
		fmt.Println(os.Args[0] + " data_feed application/bytes int 42")
		return
	}

	chanName := os.Args[1]
	contentType := os.Args[2]
	valueType := os.Args[3]
	value := os.Args[4]

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

	if valueType == "string" {
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: contentType,
				Body:        []byte(value),
			})
	} else if valueType == "int" {
		v, err := strconv.Atoi(value)
		if err != nil {
			fmt.Println(err)
			return
		}
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(v))

		fmt.Println(buf)
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: contentType,
				Body:        buf,
			})
	}
	log.Printf(" [x] Sent %s as %s with type %s", value, contentType, valueType)
	failOnError(err, "Failed to publish a message")
}
