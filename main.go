package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/streadway/amqp"
)

//FindTitle : Finds the title of the book
func FindTitle(s string) bool {
	return strings.Contains(s, "Title: ")
}

//FindBeginning : Finds the begenning of the book
func FindBeginning(s string) bool {
	s = strings.ToUpper(s)
	s = strings.Trim(s, " /n")
	words := strings.Split(s, " ")
	if len(words) > 2 {
		return false
	}
	for _, word := range words {
		if word == "I" {
			return true
		}
	}
	return false
}

//FindEnd : Finds the end of the book
func FindEnd(s string) bool {
	s = strings.ToUpper(s)
	s = strings.Trim(s, " /n")
	words := strings.Split(s, " ")
	if len(words) > 1 {
		return false
	}
	for _, word := range words {
		if word == "FIM" {
			return true
		}
	}
	return false
}

//Standarizer : Text processing
func Standarizer(s string) string {
	s = strings.Trim(s, " /n")
	r := s
	r = strings.ToLower(r)

	//Replace contractions
	r = strings.Replace(r, "d'e", "de e", -1)
	r = strings.Replace(r, "d'a", "de a", -1)

	//Replace special characters
	re := regexp.MustCompile("[[:^ascii:]]")
	r = re.ReplaceAllLiteralString(r, "")
	removelist := [...]string{"\"", "!", "@", "#", "$", "%", "¨", "&", "*", "(", ")", "--", "_", "=", "+", "[", "{", "]", "}", ",", "<", ".", ">", ";", ":", "/", "?", "|", "'"}
	for x := range removelist {
		r = strings.Replace(r, removelist[x], "", -1)
	}

	//Replace Sufixes
	r = strings.Replace(r, "-", " ", -1)

	return r
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

//DownloadFile : Download file from url
func DownloadFile(filepath string, url string) error {

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return err
}

func main() {

	//Carregamento do texto
	if len(os.Args) < 2 {
		fmt.Println("Please insert the file name.")
		return
	}

	outType := "txt"
	fileURL := os.Args[1]
	fileDir := strings.Split(fileURL, "/")
	fileName := fileURL
	if len(fileDir) > 1 {
		log.Print("Downloading file ...")
		fileName = fileDir[len(fileDir)-1]
		if err := DownloadFile(fileName, fileURL); err != nil {
			panic(err)
		}
	}
	if len(os.Args) > 2 {
		if os.Args[2] == "json" {
			outType = "json"
		}
	}

	file, err := os.Open(fileName)

	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	beginning := false
	title := ""
	processedtext := ""

	//Processamento do texto

	for scanner.Scan() {
		x := scanner.Text()
		if FindEnd(x) {
			break
		}
		if FindTitle(x) {
			title = strings.Replace(x, "Title: ", "", 1)
			processedtext += Standarizer(title)
			processedtext += "\n"
		}
		if beginning && len(x) > 0 {
			processedtext += "\n"
			processedtext += Standarizer(x)
		}
		if FindBeginning(x) {
			processedtext += x
			beginning = true
		}

	}

	//Prepara mensagens

	lines := strings.Split(processedtext, "\n")
	lines = lines[2:]
	lines = append(lines, "<EoF>")

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

	//Envia mensagens

	for _, body := range lines {
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent, //significa que a mensagem vai permanecer em uma fila após o reinicio do servidor rabbit
				ContentType:  "text/plain",
				Body:         []byte(body),
			})
		failOnError(err, "Failed to publish a message")
		//log.Printf(" [x] Sent: %s", body)
	}

	//Recebe resposta

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
		r.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var result []byte

	for d := range msgs {
		//log.Printf("Received a message: %s", string(d.Body))
		result = d.Body
		log.Printf("Done")
		d.Ack(false)
		break
	}

	//Escrever arquivo de saida
	if outType == "txt" {
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

		f, err := os.Create("wfc-" + title + ".txt")
		if err != nil {
			fmt.Println(err)
			return
		}
		l, err := f.WriteString(string(result))
		if err != nil {
			fmt.Println(err)
			f.Close()
			return
		}
		fmt.Println(l, "bytes written successfully")
		err = f.Close()
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	if outType == "json" {
		_ = ioutil.WriteFile("wfc-"+title+".json", result, 0644)
	}

}
