# 测试用例

## direct

1. 测试正常 publish -> consume -> ack -> 查看数据库是否有数据 (ok)
2. 测试 publish -> consume -> nack 查看是否重试 (ok)
3. 测试 publish -> consume 不 ack 查看是否重试 (ok)
4. 测试 delay publish 查看是否插入 delay 表 (ok)
5. 重平衡测试 (ok)

## topic

1. 多 consumer 测试正常 publish -> consume -> ack -> 查看数据库是否有数据 (ok)
2. 多 consumer 测试 publish -> consume -> nack 查看是否重试 (ok)
3. 多 consumer 测试 publish -> consume 不 ack 查看是否重试 (ok)
4. 多 consumer 测试 delay publish 查看是否插入 delay 表 (ok)
5. 重平衡测试 (ok)