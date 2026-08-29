.PHONY: test test-race test-redis lint fmt

test: ## run the unit suite (miniredis)
	go test ./... -count=1

test-race: ## run the unit suite with the race detector
	go test ./... -race -count=1 -timeout 300s

test-redis: ## run the FULL suite against a real Redis/Valkey server
	@test -n "$$REDIS_ADDR" || (echo "usage: REDIS_ADDR=localhost:6379 make test-redis" && exit 1)
	DISTSYNC_TEST_REDIS_ADDR=$$REDIS_ADDR go test ./... -race -count=1 -timeout 600s

lint: ## golangci-lint (standard set)
	golangci-lint run ./...

fmt: ## fail if any file is not gofmt-formatted
	@test -z "$$(gofmt -l .)"

coverage: ## print statement coverage and fail below 75%
	go test . -count=1 -coverprofile=coverage.out -timeout 300s
	go tool cover -func=coverage.out | tail -1
	@go tool cover -func=coverage.out | awk '$$1=="total:"{gsub("%","",$$3); if ($$3+0 < 75) { print "coverage too low: " $$3 "%"; exit 1 }}'
