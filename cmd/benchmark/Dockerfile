# syntax=docker/dockerfile:1

# 构建上下文必须是仓库根目录：
# docker build -f cmd/benchmark/Dockerfile -t prizeforge-benchmark:local .
FROM golang:1.25-bookworm AS builder

WORKDIR /src

ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" \
    -o /out/prizeforge-benchmark ./cmd/benchmark

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 prizeforge \
    && useradd --uid 10001 --gid prizeforge --no-create-home --shell /usr/sbin/nologin prizeforge

WORKDIR /app

ENV TZ=Asia/Shanghai

COPY --from=builder --chown=prizeforge:prizeforge \
    /out/prizeforge-benchmark /usr/local/bin/prizeforge-benchmark

USER prizeforge
STOPSIGNAL SIGTERM

ENTRYPOINT ["prizeforge-benchmark"]
CMD ["--help"]
