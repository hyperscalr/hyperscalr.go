.PHONY: build protocol

build:
	go build -o build/hyperscalr-ingress-api ./

protocol:
	flatc --gen-mutable --go-namespace flatbuf --filename-suffix .gen --gen-onefile --go -o ./flatbuf protocol/queue_message.fbs
