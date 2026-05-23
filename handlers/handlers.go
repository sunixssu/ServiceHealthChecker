package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"healthChecker/storage"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
)

/*
Программа отправляет запрос на сайт, получает статус код
*/

var strg storage.Storage = *storage.NewStorage()
var wg sync.WaitGroup
var maximum_goroutines_amount_str string

func SendRequest(req *http.Request, wg *sync.WaitGroup, client *http.Client, ch chan int) {
	defer func() {
		wg.Done()
		<-ch
	}()
	_, err := client.Do(req)
	if err != nil {
		fmt.Println("Error while sending request", req.URL)
	}
}

/*
Запускаем через /health
Каждые 10 секунд отправляется запрос на все ссылки из data.env
*/

func LoopServeHealth(w http.ResponseWriter, r *http.Request, delay int) {
	counter := 0
	for {
		counter++
		client := &http.Client{}
		req, err := http.NewRequest("GET", "http://localhost:8080/healthSingle", nil)
		if err != nil {
			fmt.Println("Error while trying to send request")
		}
		wg.Add(1)
		go SendRequest(req, &wg, client, make(chan int, 1))
		msg := ("Request #" + strconv.Itoa(counter) + " was successfull. Waiting " + strconv.Itoa(delay) + " seconds...")
		fmt.Println(msg)
		time.Sleep(time.Duration(delay * int(time.Second)))
	}
}

func HandleHealth(w http.ResponseWriter, r *http.Request) {
	if err := godotenv.Load("data.env"); err != nil {
		fmt.Println("Error loading data.env")
	}
	/*
		duration_str := os.Getenv("PERIOD")
		duration_int, err := strconv.Atoi(duration_str)
	*/

	// Пробовать через WebSocket?
}

func HandleHealthSingle(w http.ResponseWriter, r *http.Request) {
	if err := godotenv.Load("data.env"); err != nil {
		fmt.Println("Error loading data.env")
	}
	maximum_goroutines_amount_str = os.Getenv("MAX_GOROUTINES")
	maximum_goroutines_amount, err := strconv.Atoi(maximum_goroutines_amount_str)

	if err != nil {
		fmt.Println("Error converting string to int")
	}
	ch := make(chan int, maximum_goroutines_amount)

	err = godotenv.Load("data.env")
	if err != nil {
		fmt.Println("Error. Can't load data.env...")
	}
	addresses := os.Getenv("ADDRESSES")              // получили строку из data.env
	addresses_array := strings.Split(addresses, ",") // массив из ссылок, которые надо чекнуть

	for _, url := range addresses_array {
		wg.Add(1)
		ch <- 1
		client := &http.Client{}
		req, err := http.NewRequest("GET", "http://localhost:8080/send", bytes.NewBuffer([]byte(url)))
		if err != nil {
			fmt.Println("Error. Can't create a new request")
		}
		go SendRequest(req, &wg, client, ch)

		// Добавить лимит на количетсво одновременно запущенных горутин.
	}
	wg.Wait()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("All URLs were successfully tested"))
}

func HandleSend(w http.ResponseWriter, r *http.Request) {
	var url string
	var lst storage.Statuslist = *storage.NewStatuslist()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Println("Error, can't read from request body!")
	}
	url = string(body[:])

	fmt.Println("Я получил ссылку:", url)
	resp, err := http.Get(url)
	if err != nil {
		now := time.Now().Format("2006-01-02 15:04:05")
		fmt.Println("Пытаюсь записать", url)
		lst.AddStatus(url, 404, "NON-EXISTENT", now)
		fmt.Println("Успешно записал", url)
	} else {
		status := resp.StatusCode
		now := time.Now().Format("2006-01-02 15:04:05")
		lst.AddStatus(url, status, resp.Status[4:], now)
		fmt.Println("Успешно записал", url)
	}
	strg.AddRequest(lst)
}

func HandleGetStatus(w http.ResponseWriter, r *http.Request) {
	data, err := json.MarshalIndent(strg, "", "    ")
	if err != nil {
		fmt.Println("Error converting storage to JSON")
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(data); err != nil {
		fmt.Println("Error writing data to response body")
	}
}
