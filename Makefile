test-dep-up:
	docker-compose -f docker-compose-test.yml up -d

test-dep-down:
	docker-compose -f docker-compose-test.yml down --remove-orphans

test-cov: test-dep-up
	go test -v -race -coverprofile outbox.out ./...
	go tool cover -html=outbox.out

test: test-dep-up
	go test -v -race ./...
