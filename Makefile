export GOPATH=$(shell pwd)
DEPLOY=/Volumes/Public
SRC=b2.go

clean:
	@rm -rf bin/

vendor/github.com/cheggaaa/pb:
	git clone git@github.com:cheggaaa/pb vendor/github.com/cheggaaa/pb
	git --git-dir vendor/github.com/cheggaaa/pb/.git checkout 2af8bbdea9e99e83b3ac400d8f6b6d1b8cbbf338

get: vendor/github.com/cheggaaa/pb

bin/darwin/b2:
	@GOOS=darwin go build -o bin/darwin/b2 b2.go

bin/linux/b2:
	@GOOS=linux go build -o bin/linux/b2 b2.go

build: get bin/darwin/b2 bin/linux/b2

/Volumes/Public/b2:
	@cp -v bin/linux/b2 /Volumes/Public/b2

deploy: /Volumes/Public/b2
