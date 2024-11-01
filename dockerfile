FROM golang:1.17-alpine
WORKDIR /app

COPY go.mod ./
COPY go.sum ./
COPY cmd ./cmd
COPY internal ./internal

RUN go mod download

RUN go build ./cmd/invsvc
RUN go build ./cmd/impsvc