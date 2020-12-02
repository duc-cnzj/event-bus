# 微服务消息总线

![build-sdk](https://github.com/DuC-cnZj/event-bus/workflows/build-sdk/badge.svg)

## feature

1. 秒级延迟队列
2. grpc 任意客户端语言接入
3. 消息重试机制
4. 分布式锁
5. 高性能： 生产消费的瓶颈 = `rabbitmq` 本身生产消费的瓶颈
6. 断线重连(60s)
7. 重平衡

## v1 架构

![v1 event-bus](./images/event-bus-v1.png)

## v2 架构

![v2 event-bus](./images/event-bus-v2.png)

## Usage

```shell script
 dk run --rm -v $(pwd)/config.yaml:/config.yaml registry.cn-hangzhou.aliyuncs.com/duc-cnzj/event-bus
```

## Configuration

```yaml
Debug: false
PrintConfig: false

# 重试次数
RetryTimes: 3

# 分布式锁加锁时长
DLMExpiration: 1800

# 每个job最大执行时间
MaxJobRunningSeconds: 1800

# nackd 之后重新推送的延迟时间
NackdJobNextRunDelaySeconds: 600

# nack 以及 未消费队列重新推送定时任务(cronjob)
CronRepublishEnabled: true

# 延迟队列定时任务(cronjob)
CronDelayPublishEnabled: true

# 是否开启后台 event_bus_ack_queue 和 event_bus_confirm_queue 消费队列, 保持默认 `true` 就行
BackgroundConsumerEnabled: true

# 后台 event_bus_ack_queue  event_bus_confirm_queue 定时任务 的协程数量
# 默认 10 个就行了，根据数据库处理能力定, BackgroundConsumerEnabled 为 false 不执行
BackConsumerGoroutineNum: 20

AmqpUrl: amqp://guest:guest@localhost:5672/
PrefetchCount: 0
HttpPort: 7878
RpcPort: 9091

# redis
RedisAddr: localhost:6379
RedisDB: 0
RedisUsername:
RedisPassword:

# DATABASE
DB_HOST: 127.0.0.1
DB_PORT: 3306
DB_DATABASE: mq_v2
DB_USERNAME: root
DB_PASSWORD:

# 每个队列的消费者数量
EachQueueConsumerNum: 5

# 每个队列的生产者数量
EachQueueProducerNum: 2
```

## SDK

php laravel, 配置 env 例如：`MQ_HOST=localhost:9091`
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
4. 后台 `event_bus_ack_queue` 、 `event_bus_confirm_queue` 、`event_bus_delay_queue` 处理速率，这些操作都是入库操作，和数据库本身性能有关系

### 重点关注

1. 100w 数据量时队列延迟情况
2. 各种有可能丢数据的骚操作，会不会真的丢数据
3. 是否存在内存泄露的情况
4. 数据库可能出现连接过多的问题，注意数据库的最大连接数

## 生产过快导致消费缓慢

https://www.rabbitmq.com/blog/2011/09/24/sizing-your-rabbits/

## 注意事项

1. 关注数据库的最大连接数，防止 `too many connections` 错误导致入库失败


## 重平衡 

> 分布式环境下不同节点都会有个消费者消费，但是因为 prefetch 的存在，导致消费者本身会有消息缓存，如果出现 a 容器的消费者的消息缓存是0, b 容器的消费者还有消息多余，那么因为负载均衡的机制，会导致客户端block在a容器这里，导致 b 容器的剩余消息无法被消费，所以需要重平衡。

实现方式
1. 后台监听，如果客户端因为当前节点 block 则往 重平衡队列里推一条消息 (fanout)，其它容器会收到这条消息，如果其它容器的对应队列本地还有数据，则触发重平衡 