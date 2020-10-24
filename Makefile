shell := /bin/bash

test:
	go test -v -cover ./...

cover:
	go test -cover -coverprofile=cover.out -v ./... \
	&& go tool cover -html=cover.out -o cover.html \
	&& rm cover.out

clean:
	rm -f cover.out cover.html