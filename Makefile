IMAGE   ?= hub.global.cloud.sap/monsoon/maria-back-me-up
VERSION = $(shell git rev-parse --verify HEAD | head -c 8)

GOOS    ?= $(shell go env | grep GOOS | cut -d'"' -f2)
BINARY  := maria-back-me-up

LDFLAGS := -X github.com/sapcc/maria-back-me-up/pkg/maria-back-me-up.VERSION=$(VERSION)
GOFLAGS := -ldflags "$(LDFLAGS)"

SRCDIRS  := cmd pkg internal
PACKAGES := $(shell find $(SRCDIRS) -type d)
GOFILES  := $(addsuffix /*.go,$(PACKAGES))
GOFILES  := $(wildcard $(GOFILES))


all: bin/$(GOOS)/$(BINARY)

bin/%/$(BINARY): $(GOFILES) Makefile
	GOOS=$* GOARCH=amd64 go build $(GOFLAGS) -v -i -o bin/$*/$(BINARY) ./cmd

build: 
	docker build -t $(IMAGE):$(VERSION) .

push: build
	docker push $(IMAGE):$(VERSION)

clean:
	rm -rf bin/*

vendor:
	GO111MODULE=on go get -u ./... && go mod tidy && go mod vendor
