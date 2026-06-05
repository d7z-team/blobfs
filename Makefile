GOCACHE ?= $(CURDIR)/.gocache
GOFILES := $(shell find . -name '*.go' -not -path './.git/*')

.PHONY: fmt test race vet cover lint

fmt:
	@gofmt -w $(GOFILES)

test:
	@GOCACHE=$(GOCACHE) go test ./...

race:
	@GOCACHE=$(GOCACHE) go test -race ./...

vet:
	@GOCACHE=$(GOCACHE) go vet ./...

cover:
	@GOCACHE=$(GOCACHE) go test -cover ./...

lint:
	@(test -f "$(GOPATH)/bin/golangci-lint" || go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.6.0) && \
    "$(GOPATH)/bin/golangci-lint" run -c ./.golangci.yml ./...
