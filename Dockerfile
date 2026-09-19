FROM golang:1.26-alpine3.23 as builder

WORKDIR /app
COPY . .

ARG GITHUB_TOKEN
ENV GOPRIVATE=github.com/MBI-88/*

RUN apk add --no-cache git

RUN git config --global url."https://${GITHUB_TOKEN}:x-oauth-basic@github.com/".insteadOf "https://github.com/"

RUN go mod download && \
    CGO_ENABLED=0 GOOS=linux go build  -ldflags="-s -w" -o ./consumer ./main.go

FROM alpine:3.23.3 as deployment
WORKDIR /app

ARG PORT
ENV DOMINUS_URL="127.0.0.1:5000"
ENV API_KEY="dominus-api-key-1233464687"
COPY --from=builder /app/consumer ./consumer

RUN addgroup -S appgroup && adduser -S appuser -G appgroup && \
    chown appuser:appgroup /app

CMD ["/consumer", PORT]