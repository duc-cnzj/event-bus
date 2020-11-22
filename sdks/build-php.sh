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
cat composer.json && \
ls src && \
cat src/Mq/Mq.php && \
composer update && ./vendor/bin/rpc-generator $(pwd)