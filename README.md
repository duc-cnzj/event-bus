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

## v1 架构

![v1 event-bus](./images/event-bus-v1.png)

## v2 架构

![v2 event-bus](./images/event-bus-v2.png)

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

## TODO

1. 关闭长时间没用的连接

## test case

1. 普通队列/延迟队列：持续压测关注 qos 情况
2. 消费时重启 mq、重启 event-bus，查看数据是否有丢失
3. 后台 job: `republish` `delay publish` 的处理速率
4. 后台 `event_bus_ack_queue` 、 `event_bus_confirm_queue` 、`delay publish` 处理速率

### 重点关注

1. 100w 数据量时队列延迟情况
2. 各种有可能丢数据的骚操作，会不会真的丢数据
3. 是否存在内存泄露的情况
4. 数据库可能出现连接过多的问题，注意数据库的最大连接数