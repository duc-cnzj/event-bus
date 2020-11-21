FROM golang:1.15-alpine AS builder

WORKDIR /app

COPY . .

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories && \
  apk add --no-cache ca-certificates tzdata

RUN go env -w GOPROXY=https://goproxy.cn && \
    go env -w GO111MODULE=on && \
    go mod tidy -v

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-w -s" -o /bin/app main.go

FROM alpine

WORKDIR /

COPY --from=builder /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /bin/app /bin/app

CMD ["app", "help"]