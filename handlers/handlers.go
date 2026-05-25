package handlers

import (
	"encoding/json"
	"fmt"
	"healthChecker/storage"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type EnvironmentData struct {
	Addresses      string
	Max_Goroutines int
	Period         int
}

func NewEnvironmentData(a string, m int, p int) *EnvironmentData {
	return &EnvironmentData{
		Addresses:      a,
		Max_Goroutines: m,
		Period:         p,
	}
}

var strg storage.Storage = *storage.NewStorage()

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}
var mtx sync.Mutex

/*
Запускаем через /health
Каждые 10 секунд отправляется запрос на все ссылки из data.env
*/

func (ed EnvironmentData) HandleHealth(w http.ResponseWriter, r *http.Request) {
	var wgAllURLsCheckWasFinished sync.WaitGroup
	duration_int := ed.Period
	duration_str := strconv.Itoa(duration_int)
	addresses := ed.Addresses
	addresses_array := strings.Split(addresses, ",")
	if len(addresses_array) == 0 {
		http.Error(w, storage.EmptyAddressList, http.StatusBadRequest)
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, storage.ErrConnectingToWS, http.StatusInternalServerError)
		//fmt.Println("Error connecting to WebSocket:", err)
		return
	}
	ws.SetReadDeadline(time.Now().Add(60 * time.Second))
	defer ws.Close()

	counter := 0
	for {

		wgAllURLsCheckWasFinished.Add(1)

		CheckAllURLsFromDataCycle(addresses_array, make(chan int, ed.Max_Goroutines), &wgAllURLsCheckWasFinished, true)

		counter++

		ws.WriteMessage(websocket.TextMessage, []byte("Request #"+strconv.Itoa(counter)+" was successfull, waiting "+duration_str+" seconds..."))
		time.Sleep(time.Duration(duration_int * int(time.Second)))
	}
}

// Эта функция для проверки всего списка функций из data.env, для КАЖДОЙ ссылки вызывается проверка. Вызывается /send
func (ed EnvironmentData) HandleHealthSingle(w http.ResponseWriter, r *http.Request) {
	var wgAllURLsCheckWasFinished sync.WaitGroup

	maximum_goroutines_amount := ed.Max_Goroutines
	chMaxGrts := make(chan int, maximum_goroutines_amount)

	addresses := ed.Addresses
	addresses_array := strings.Split(addresses, ",")

	wgAllURLsCheckWasFinished.Add(1)

	CheckAllURLsFromDataCycle(addresses_array, chMaxGrts, &wgAllURLsCheckWasFinished, true)

	wgAllURLsCheckWasFinished.Wait()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("All URLs were successfully tested"))
}

// Эта функция проверяет весь список URL из data.env, для каждой ссылки вызывается проверка. И еще отметку делает
func CheckAllURLsFromDataCycle(array_of_addresses []string, chMaxGrts chan int, wgAllURLsFinished *sync.WaitGroup, flag bool) {
	var wgMaxGrts sync.WaitGroup
	for _, url := range array_of_addresses {
		wgMaxGrts.Add(1)
		chMaxGrts <- 1

		go SendReqToURL(&wgMaxGrts, chMaxGrts, url)
	}
	wgMaxGrts.Wait()
	if flag {
		wgAllURLsFinished.Done()
	}
}

// Эта функция для проверки ОДНОЙ ссылки из data.env, на проверку ее ответа.
func SendReqToURL(wg *sync.WaitGroup, chMaxGrts chan int, url string) {
	var lst storage.Statuslist = *storage.NewStatuslist()

	fmt.Println("Я получил ссылку:", url)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	req, err := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
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
	mtx.Lock()
	strg.AddRequest(lst)
	mtx.Unlock()
	wg.Done()
	<-chMaxGrts
}

func HandleGetStatus(w http.ResponseWriter, r *http.Request) {
	data, err := json.MarshalIndent(strg, "", "    ")
	if err != nil {
		//fmt.Println("Error converting storage to JSON")
		http.Error(w, storage.ErrConvertingToJSON, http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(data); err != nil {
		fmt.Println("Error writing data to response body")
	}
}
