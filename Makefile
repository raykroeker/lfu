BIN     := lfu
PKG     := lfu
ROOT    := $(shell pwd)
SRC     := $(shell find src -type f)
VERSION := 0.0.1

# make clean build && cp bin/linux/lfu /Volumes/Public/

GOPATH  := $(ROOT)
LDFLAGS := "-X main.version=$(VERSION)"
VENDOR  := "src/vendor"

PHONY: _env get

_env:
	env

clean:
	@rm -rf bin/

src/vendor/golang.org/x/sys:
	git clone git@github.com:golang/sys src/vendor/golang.org/x/sys
	git --git-dir src/vendor/golang.org/x/sys/.git checkout c11f84a56e43e20a78cee75a7c034031ecf57d1f

src/vendor/github.com/mattn/go-runewidth:
	git clone git@github.com:mattn/go-runewidth src/vendor/github.com/mattn/go-runewidth
	git --git-dir src/vendor/github.com/mattn/go-runewidth/.git checkout ce7b0b5c7b45a81508558cd1dba6bb1e4ddb51bb

src/vendor/github.com/cheggaaa/pb:
	git clone git@github.com:cheggaaa/pb src/vendor/github.com/cheggaaa/pb
	git --git-dir src/vendor/github.com/cheggaaa/pb/.git checkout 2af8bbdea9e99e83b3ac400d8f6b6d1b8cbbf338

get: src/vendor/golang.org/x/sys src/vendor/github.com/mattn/go-runewidth src/vendor/github.com/cheggaaa/pb

bin/darwin/$(BIN):
	@GOPATH=$(GOPATH) GOOS=darwin go build -o bin/darwin/$(BIN) $(PKG)/cmd

bin/linux/$(BIN):
	@GOPATH=$(GOPATH) GOOS=linux go build -o bin/linux/$(BIN) $(PKG)/cmd

build: bin/darwin/$(BIN) bin/linux/$(BIN)
