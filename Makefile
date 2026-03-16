.PHONY: test test-race fmt vet mockery

test:
	go test ./...

test-race:
	go test -race ./...

fmt:
	gofmt -w .
	goimports -w .

vet:
	go vet ./...

mockery:
	mockery --config .mockery.yml
