#!/usr/bin/env bash

protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    *.proto

if [ ! -d ./php/src ]; then
  mkdir ./php/src
fi
protoc --proto_path=. --php_out=./php/src --grpc_out=./php/src --plugin=protoc-gen-grpc=/usr/local/bin/grpc_php_plugin *.proto
cd ./php && composer update && ./vendor/bin/rpc-generator $(pwd)