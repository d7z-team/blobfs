GOCACHE ?= $(CURDIR)/.gocache
GOPATH ?= $(shell go env GOPATH)
GOFILES := $(shell find . -name '*.go' -not -path './.git/*')

.PHONY: fmt test race vet cover lint

fmt:
	@gofmt -w $(GOFILES)

test:
	@GOCACHE=$(GOCACHE) go test -timeout=30s ./...

race:
	@GOCACHE=$(GOCACHE) go test -race -timeout=30s ./...

vet:
	@GOCACHE=$(GOCACHE) go vet ./...

cover:
	@GOCACHE=$(GOCACHE) go test -cover -timeout=30s ./...

lint:
	@(test -f "$(GOPATH)/bin/golangci-lint" || go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.6.0) && \
    "$(GOPATH)/bin/golangci-lint" run -c ./.golangci.yml ./...
