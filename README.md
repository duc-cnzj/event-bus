# 微服务消息总线

## 生产过快导致消费缓慢

https://www.rabbitmq.com/blog/2011/09/24/sizing-your-rabbits/


## feature

1. 秒级延迟队列
2. grpc 任意语言接入
3. 消息重试机制
4. 分布式锁
5. 高性能
6. 断线重连(60s)

## Usage

```shell script
 dk run --rm -v $(pwd)/config.yaml:/config.yaml duccnzj/mq-event-bus
```

## SDK

php laravel
```shell script
composer require duc_cnzj/event-bus-sdk
```

golang
```shell script
go get -u github.com/DuC-cnZj/event-bus-proto
```

## 坑

fiber 通过 `ctx.Query("queue", "test_queue")` 拿出来的 string 被底层改过，都指向同一个地址，你前一个请求的值和会被后一个请求的值更改！！！