# Consumer Example: SDK Dominus

## Overview

This example demonstrates how to implement a consumer pattern within the MISS project framework. It showcases best practices for building message consumers that process events or data from various sources.

## Purpose

The consumer example illustrates:

- Creating a consumer to handle incoming messages or events.
- Processing and validating data from external sources.
- Implementing error handling and retry logic.
- Integrating the consumer with other MISS components.

## Key Features

- **Message Processing**: Demonstrates how to consume and process messages.
- **Error Handling**: Shows error handling and logging mechanisms.
- **Configuration**: Illustrates how to configure consumer settings.
- **Integration**: Shows how to integrate the consumer with the MISS architecture.

## Getting Started

1. Review the source code in this directory.
2. Read the SDK usage and integration flow below.
3. Configure the endpoint and API key for the Dominus broker.
4. Run the example using the command described in [Requirements and execution](#requirements-and-execution).

## Structure

This example includes:

- Consumer implementation in `internal/consumer.go`.
- Broker streaming handlers in `internal/subscriber.go`.
- Application bootstrap in `main.go`.
- Go module and dependency configuration in `go.mod`.

## More Information

For more details about consumers in the MISS project, refer to the main project documentation. The following sections describe the Dominus SDK usage implemented by this example.

## Description

This project demonstrates how to integrate a Go consumer with `dominus-broker` through `github.com/MBI-88/dominus-sdk/dominus` and gRPC. The example covers SQS-style messaging (producing, consuming, and acknowledging messages) and registering a Broker service with gRPC streams.

The versions used are `dominus-sdk v1.3.5` and `dominus-proto-definition v1.3.7`. The 15-second intervals and the API key in the code are for demonstration only and are not production settings.

## Requirements and execution

- Go `1.26.1` or compatible with the module.
- An accessible Dominus endpoint expressed as `host:port`.
- A valid API key.

The example uses `127.0.0.1:5000` as the remote endpoint and receives the local port as its first argument:

```powershell
go run . 6000
```

## SQS client

The non-TLS configuration used by the example is:

```go
idempotency := uuid.New().String()
sqs := dominus.NewSqsConfig("127.0.0.1:5000").
	InitAPIClients(apiKey, idempotency)
```

For TLS:

```go
sqs := dominus.NewSqsConfig(dominusURL).
	InitAPIClientsTLSFromFile(caCertPath, serverName, apiKey, idempotency)
```

The API key and idempotency value should be configured through environment variables or a secret manager. `InitAPIClients` should be reserved for development or trusted networks.

### Consuming and acknowledging messages

The consumer identifies a worker and a group:

```go
workerID := fmt.Sprintf("worker-%s", idempotency)
groupID := "consumer-group"

response, err := sqs.UseConsumer(&dominus.ConsumerRequest{
	WorkerId: workerID,
	GroupId:  groupID,
})
```

`ConsumerRequest` uses `WorkerId` and `GroupId` for consumption. The response exposes `GetMessageId()`, `GetDate()`, and `GetMessage()`. After the message has been processed successfully, acknowledge it with:

```go
_, err = sqs.UseAck(&dominus.ConsumerRequest{
	MessageId: response.GetMessageId(),
	WorkerId:  workerID,
	GroupId:   groupID,
})
```

The recommended flow is to request, process, and acknowledge. The ACK must only be sent after successful processing. If processing fails, the application must explicitly decide whether to retry, discard, or route the message to an error queue. The exact redelivery policy must be verified in Dominus.

The example polls every 15 seconds and waits another 15 seconds before acknowledging; these intervals are illustrative only.

### Complete consumption example

The following snippet summarizes the cycle implemented in `internal/consumer.go`, including ticker cleanup and stop-signal handling:

```go
func consume(sqs dominus.Sqs, workerID, groupID string, stop <-chan os.Signal) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			message, err := sqs.UseConsumer(&dominus.ConsumerRequest{
				WorkerId: workerID,
				GroupId:  groupID,
			})
			if err != nil {
				log.Printf("consumer error: %v", err)
				continue
			}

			log.Printf("received %s: %s", message.GetMessageId(), message.GetMessage())
			if err := process(message.GetMessage()); err != nil {
				log.Printf("processing failed for %s: %v", message.GetMessageId(), err)
				continue
			}

			_, err = sqs.UseAck(&dominus.ConsumerRequest{
				MessageId: message.GetMessageId(),
				WorkerId:  workerID,
				GroupId:   groupID,
			})
			if err != nil {
				log.Printf("ack error for %s: %v", message.GetMessageId(), err)
			}

		case <-stop:
			return
		}
	}
}
```

In this example, `process` represents the business logic and should return an error when the message cannot be processed. The message must not be acknowledged before `process` completes successfully.

### Producing messages

The `Sqs` interface also exposes:

```go
UseProducer(*dominus.ProducerRequest) (*dominus.ProducerResponse, error)
```

This repository does not include a working producer. The fields of `ProducerRequest` must be checked in the pinned version of `dominus-proto-definition`. Producers should apply limited retries and idempotency when required by the broker contract.

## Broker server and streaming

The example registers a handler compatible with the Broker API:

```go
options := dominus.NewServerOption().InitServerOption(apiKey)
register := dominus.NewBrokerRegister(options)
server := register.RegisterHandler(&brokerSvc{})

listener, err := net.Listen("tcp", address)
if err != nil {
	return err
}
return server.Serve(listener)
```

For TLS, use `InitServerOptionsTLSFromFile(certFile, keyFile, apiKey)`. The handler must embed `UnimplementedBrokerAPIServer` and satisfy the generated protobuf interfaces.

`internal/subscriber.go` demonstrates three modes:

- **Server stream**: receives one request and sends multiple responses with `stream.Send`.
- **Client stream**: receives messages with `stream.Recv` until `io.EOF`.
- **Bidirectional stream**: receives and sends independently; the example responds with `ACK`.

A Broker client is created with subscribers and an endpoint:

```go
broker := dominus.NewBrokerConfig(
	[]string{"subscriber-a:5000"},
	dominusURL,
).InitAPIClients(apiKey)
```

Its main helpers are `UseStreamClientConn`, `UseStreamServerConn`, and `UseBiStreamConn`. The `InitAPIClientsTLSFromFile` variant is available for TLS.

### Server stream example

A client opens a server stream and receives the payloads produced by `ServerStream`:

```go
receive := broker.UseStreamServerConn()

for {
	payload, err := receive()
	if err == io.EOF {
		break
	}
	if err != nil {
		log.Printf("stream receive error: %v", err)
		break
	}
	log.Printf("server payload: %s", payload)
}
```

The corresponding handler can send multiple responses and finish when the sequence is complete:

```go
func (s *brokerSvc) ServerStream(
	req *do.StreamRequestMessage,
	stream do.BrokerAPI_ServerStreamServer,
) error {
	for index := 0; index < 3; index++ {
		if err := stream.Send(&do.StreamResponseMessage{
			Payload: []byte(fmt.Sprintf("response-%d", index)),
			Status:  0,
		}); err != nil {
			return err
		}
	}
	return nil
}
```

### Client stream example

In a client stream, the client sends multiple values and closes its send side. The server continues reading until it receives `io.EOF`:

```go
send, closeSend := broker.UseStreamClientConn()

for _, value := range []string{"one", "two", "three"} {
	if err := send(map[string]string{"payload": value}); err != nil {
		log.Fatal(err)
	}
}

if err := closeSend(); err != nil {
	log.Printf("stream close error: %v", err)
}
```

The send helper accepts an `any` value and serializes it according to the SDK contract. The receiver must always check the result of `Recv` and treat `io.EOF` as normal completion.

### Bidirectional stream example

The bidirectional mode allows a message to be sent and its response to be received without immediately closing the stream:

```go
send, receive, closeSend := broker.UseBiStreamConn()
defer closeSend()

if err := send(map[string]string{"payload": "hello"}); err != nil {
	log.Fatal(err)
}

response, err := receive()
if err != nil {
	log.Fatal(err)
}
log.Printf("broker response: %s", response.GetPayload())
```

The example handler responds to each message with an `ACK` payload:

```go
if err := stream.Send(&do.StreamResponseMessage{
	Payload: []byte("ACK"),
}); err != nil {
	return err
}
```

## Shutdown and operations

The program starts the local server and the SQS consumer in parallel. When it receives `os.Interrupt` or `SIGTERM`, it calls `GracefulStop` to allow in-flight RPCs to complete.

For a production integration, we recommend:

1. Externalize and rotate the API key.
2. Prefer TLS outside trusted networks.
3. Add `context.Context`, timeouts, and backoff.
4. Stop tickers and release resources during shutdown.
5. Avoid logging sensitive messages or using `log.Fatal` in goroutines.
6. Measure requested, processed, acknowledged, and failed messages, as well as latency.
7. Validate duplicate and redelivery semantics before assuming exactly-once processing.

## Quick reference

| Necesidad | API |
| --- | --- |
| Cliente SQS sin TLS | `NewSqsConfig(dns).InitAPIClients(apiKey, idempotency)` |
| Cliente SQS con TLS | `NewSqsConfig(dns).InitAPIClientsTLSFromFile(ca, serverName, apiKey, idempotency)` |
| Publicar | `sqs.UseProducer(request)` |
| Consumir | `sqs.UseConsumer(request)` |
| Acknowledge | `sqs.UseAck(request)` |
| Cliente Broker | `NewBrokerConfig(subscribers, dns).InitAPIClients(apiKey)` |
| Register server | `NewBrokerRegister(options).RegisterHandler(handler)` |
| Servir gRPC | `server.Serve(listener)` |
| Graceful shutdown | `server.GracefulStop()` |
