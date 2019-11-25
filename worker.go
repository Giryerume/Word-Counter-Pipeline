package main

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/streadway/amqp"
)

//WordCount : Counts que frequency of words
func WordCount(words []string) map[string]int {
	// iterate over the slice of words and populate
	// the map with word:frequency pairs
	// (where word is the key and frequency is the value)
	wordFreq := make(map[string]int)
	// range string slice gives index, word pairs
	// index is not needed, so use blank identifier _
	for _, word := range words {
		// check if word (the key) is already in the map
		_, ok := wordFreq[word]
		// if true add 1 to frequency (value of map)
		// else start frequency at 1
		if ok == true {
			wordFreq[word]++
		} else {
			wordFreq[word] = 1
		}
	}
	return wordFreq
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")
	p, err := ch.QueueDeclare(
		"task_queue2", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//diz ao servidor que não entregue uma mensagem antes da anterior ter sido reconhecida
	err = ch.Qos(
		1,     // prefetch count: entrega no máximo essa quantidade de msgs antes de um reconhecimento
		0,     // prefetch size: o servidor vai tentar manter ao menos tantas msgs entregues aos consumidores antes do reconhecimento
		false, // global: quando true se aplica a todos consumidores e canais em uma conexão. Quando false só neste canal.
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			//log.Printf("Received a message: %s", d.Body)

			//recebe linhas e processa
			words := strings.Split(string(d.Body), " ")
			wordMap := WordCount(words)

			//converte map em json
			mapJSON, err := json.Marshal(wordMap)

			log.Printf("Done")
			d.Ack(false)

			//repassa os dados processados
			err = ch.Publish(
				"",     // exchange
				p.Name, // routing key
				false,  // mandatory
				false,
				amqp.Publishing{
					DeliveryMode: amqp.Persistent, //significa que a mensagem vai permanecer em uma fila após o reinicio do servidor rabbit
					ContentType:  "text/plain",
					Body:         mapJSON,
				})
			failOnError(err, "Failed to publish a message")
			//log.Printf(" [x] Sent: %s", map_json)
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
