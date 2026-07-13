package main

import (
	"consumer/internal"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	
	"github.com/MBI-88/dominus-sdk/dominus"
	"github.com/google/uuid"
)

const (
	API_KEY     = "dominus-api-key-1233464687"
	DOMINUS_URL = "127.0.0.1:5000"
)

func brokerListening(server dominus.Server, port int64) {
	fmt.Printf("Broker is listening on %d\n", port)
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Fatal(err)
	}

	if err := server.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

func sqsClient(url string, stop <-chan os.Signal) {
	idempotency := uuid.New().String()
	sdk := dominus.NewSqsConfig(url).InitAPIClients(API_KEY, idempotency)
	workerId := fmt.Sprintf("worker-%s", idempotency)
	consumer := internal.NewConsumer(sdk, workerId, "consumer-group")

	consumer.GetMessage(stop)

}

func main() {
	flag.Parse()
	args := flag.Arg(0)
	
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	port, err := strconv.ParseInt(args, 10, 64)
	if err != nil {
		panic(err)
	}
	opts := dominus.NewServerOption().InitServerOption(API_KEY)
	reg := dominus.NewBrokerRegister(opts)
	srv := internal.NewBrokerSvc(reg)
	go brokerListening(srv, port)

	//go sqsClient(DOMINUS_URL, stop)

	<-stop
	srv.GracefulStop()
	close(stop)

	fmt.Println("[+] System stopped graceful!")

}
