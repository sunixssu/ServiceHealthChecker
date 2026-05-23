package main

import (
	"healthChecker/handlers"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
	r := mux.NewRouter()
	//r.HandleFunc("/health", handlers.HandleHealth)
	r.HandleFunc("/healthSingle", handlers.HandleHealthSingle)
	r.HandleFunc("/send", handlers.HandleSend)
	r.HandleFunc("/status", handlers.HandleGetStatus)
	http.Handle("/", r)

	http.ListenAndServe(":8080", r)
}
