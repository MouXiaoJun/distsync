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
