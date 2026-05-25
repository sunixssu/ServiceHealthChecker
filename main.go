package main

import (
	"context"
	"fmt"
	"healthChecker/handlers"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
)

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	r := mux.NewRouter()

	s := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	if err := godotenv.Load("data.env"); err != nil {
		fmt.Println(err)
	}
	var err error
	var addresses_env string = os.Getenv("ADDRESSES")
	var max_grts_env int
	var period_env int
	if max_grts_env, err = strconv.Atoi(os.Getenv("MAX_GOROUTINES")); err != nil {
		fmt.Println("Error, can't convert string to int")
		return
	}
	if period_env, err = strconv.Atoi(os.Getenv("PERIOD")); err != nil {
		fmt.Println("Error, can't convert string to int")
		return
	}
	envData := *handlers.NewEnvironmentData(addresses_env, max_grts_env, period_env)

	r.HandleFunc("/health", envData.HandleHealth)
	r.HandleFunc("/healthSingle", envData.HandleHealthSingle)
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
