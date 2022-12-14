# init command
GO     := go
GOROOT := $(shell $(GO) env GOROOT)
GOMOD  := $(GO) mod
GOTEST := $(GO) test -v -race
PKGRAFT := "6.824/raft"

all:lab2

# set go env
set-env:
	$(GO) env -w GO111MODULE=on
	$(GO) env -w GOPROXY=https://goproxy.io,direct
	$(GO) env -w GONOSUMDB=off

# make prepare
prepare: gomod
gomod: set-env
	$(GOMOD) tidy

lab2: lab2a lab2b lab2c lab2d
lab2a: prepare
	$(GOTEST) --run 2A $(PKGRAFT)

lab2b: prepare
	$(GOTEST) --run 2B $(PKGRAFT)

lab2c: prepare
	$(GOTEST) --run 2C $(PKGRAFT)

lab2d: prepare
	$(GOTEST) --run 2D $(PKGRAFT)

# avoid filename conflict and speed up build
.PHONY: all prepare lab2 lab2a lab2b lab2c lab2d
