build: build-protos build-php-sdk

build-protos:
	cd protos && \
	protoc --go_out=. --go_opt=paths=source_relative \
        --go-grpc_out=. --go-grpc_opt=paths=source_relative \
        *.proto \
	&& ls -al

build-php-sdk:
	cd sdks && bash build-php.sh