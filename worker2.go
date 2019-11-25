package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

//MergeMap : Merges two maps into one
func MergeMap(m1, m2 map[string]int) map[string]int {
	for k, v := range m1 {
		if v2, ok := m2[k]; ok {
			v += v2
		}
		m2[k] = v

	}
	return m2
}

//ResetMap : Resets a map
func ResetMap(m map[string]int) {
	for k := range m {
		delete(m, k)
	}
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

	p, err := ch.QueueDeclare(
		"task_queue2", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare a queue")
	r, err := ch.QueueDeclare(
		"resp_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
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
		p.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
	var resultMap map[string]int

	go func() {
		for d := range msgs {
			//log.Printf("Received a message: %s", d.Body)

			var aux map[string]int
			err := json.Unmarshal(d.Body, &aux)
			if err != nil {
				fmt.Println("error:", err)
			}
			resultMap = MergeMap(resultMap, aux)

			log.Printf("Done")
			d.Ack(false)

			if _, ok := aux["<EoF>"]; ok {
				delete(aux, "<EoF>")
				delete(aux, "")
				b, err := json.MarshalIndent(resultMap, "", "  ")
				if err != nil {
					fmt.Println("error:", err)
				}
				//fmt.Println(string(b))
				err = ch.Publish(
					"",     // exchange
					r.Name, // routing key
					false,  // mandatory
					false,
					amqp.Publishing{
						DeliveryMode: amqp.Persistent, //significa que a mensagem vai permanecer em uma fila após o reinicio do servidor rabbit
						ContentType:  "text/plain",
						Body:         b,
					})
				failOnError(err, "Failed to publish a message")
				ResetMap(resultMap)
			}
			//log.Printf(" [x] Sent: %s", map_json)
		}

	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
