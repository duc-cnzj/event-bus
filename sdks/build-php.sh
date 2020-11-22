#!/usr/bin/env bash

if [ ! -d ./php/src ]; then
  mkdir ./php/src
fi
protoc --proto_path=../protos/  --proto_path=. --php_out=./php/src --grpc_out=./php/src --plugin=protoc-gen-grpc=/usr/local/bin/grpc_php_plugin ../protos/*.proto

cd ./php &&  \
cp composer.json src/DucCnzj && \
cd src/DucCnzj && \
mv EventBus src && \
pwd && \
ls && \
ls src && \
cat composer.json && \
cat src/Mq/Mq.php && \
cat src/Mq/DelayPublishRequest.php && \
composer update && ./vendor/bin/rpc-generator $(pwd)