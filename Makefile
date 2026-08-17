.PHONY: build vet test race ci demo

build:
	go build ./...

vet:
	go vet ./...

test:
	go test ./...

race:
	go test -race ./...

ci: build vet test race

demo:
	go run ./cmd/demo
