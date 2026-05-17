FROM golang:1.24 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN GOPROXY=https://goproxy.cn go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-X main.Version=docker" -o /out/big-market-kratos ./cmd/big-market-kratos
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/cdc-sync ./cmd/cdc-sync

FROM debian:stable-slim AS server

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        netbase \
        && rm -rf /var/lib/apt/lists/* \
        && apt-get autoremove -y \
        && apt-get autoclean -y

COPY --from=builder /out/big-market-kratos /app/big-market-kratos

WORKDIR /app

EXPOSE 8000
EXPOSE 9000
VOLUME /data/conf

CMD ["./big-market-kratos", "-conf", "/data/conf"]

FROM debian:stable-slim AS cdc-sync

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        netbase \
        && rm -rf /var/lib/apt/lists/* \
        && apt-get autoremove -y \
        && apt-get autoclean -y

COPY --from=builder /out/cdc-sync /app/cdc-sync

WORKDIR /app

CMD ["./cdc-sync"]
