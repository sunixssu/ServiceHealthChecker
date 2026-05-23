package main

import (
	"context"
	"fmt"
	"healthChecker/handlers"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
)

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	r := mux.NewRouter()

	s := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	r.HandleFunc("/health", handlers.HandleHealth)
	r.HandleFunc("/healthSingle", handlers.HandleHealthSingle)
	r.HandleFunc("/send", handlers.HandleSend)
	r.HandleFunc("/status", handlers.HandleGetStatus)
	http.Handle("/", r)

	go func() {
		if err := s.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Println("Server fatal error:", err)
		}
	}()

	<-signalChan

	ctxShutDown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Shutdown(ctxShutDown); err != nil {
		fmt.Println("Server shut down:", err)
	}
	fmt.Println("Server stopped.")
}
